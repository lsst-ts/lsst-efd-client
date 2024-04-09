"""EFD client."""

import datetime
import logging
import re
from enum import Enum
from functools import partial
from typing import Any
from urllib.parse import urljoin

import aiohttp
import aioinflux
import astropy.units as u
import pandas as pd
import requests
from astropy.time import Time, TimeDelta
from kafkit.registry.aiohttp import RegistryApi
from lsst.utils.iteration import ensure_iterable

from .auth_helper import NotebookAuth
from .efd_utils import (
    SyncSchemaParser,
    check_time_format,
    efd_timestamp_to_astropy,
    get_begin_end,
    get_day_obs_start_time,
    merge_packed_time_series,
)

# When looking backwards in time to find the most recent state event, look back
# in chunks of this size. Too small, and there will be too many queries, too
# large and there will be too much data returned unnecessarily, as we only need
# one row by definition. Will tune this parameters in consultation with SQuaRE.
TIME_CHUNKING = datetime.timedelta(minutes=15)


class ClientMode(Enum):
    ASYNC = "async"
    SYNC = "blocking"


class _EfdClientStatic:
    """Static tools for EfdClient"""

    influx_client = None
    """The `aioinflux.client.InfluxDBClient` used for queries.

    This should be used to execute queries not wrapped by this class.
    """

    subclasses = {}  # type: ignore
    deployment = ""

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """Register subclasses with the abstract base class."""
        super().__init_subclass__(**kwargs)
        if cls.mode in _EfdClientStatic.subclasses:
            raise ValueError(f"Class for mode, {cls.mode}, already defined")
        _EfdClientStatic.subclasses[cls.deployment] = cls

    @classmethod
    def list_efd_names(
        cls, creds_service="https://roundtable.lsst.codes/segwarides/"
    ):
        """List all valid names for EFD deployments available.

        Parameters
        ----------
        creds_service : `str`, optional

        Returns
        -------
        results : `list`
            A `list` of `str` each specifying the name of a valid deployment.
        """
        return NotebookAuth(service_endpoint=creds_service).list_auth()

    def from_name(self, efd_name, *args, **kwargs):
        """Construct a client for the specific named subclass.

        Parameters
        ----------
        efd_name : `str`
            Name of the EFD instance for which to construct a client.
        *args
            Extra arguments to pass to the subclass constructor.
        **kwargs
            Extra keyword arguments to pass to the subclass constructor.

        Raises
        ------
        NotImplementedError
            Raised if there is no subclass corresponding to the name.
        """
        if efd_name not in self.subclasses:
            raise NotImplementedError(
                f"There is no EFD client class implemented for {efd_name}."
            )
        return self.subclasses[efd_name](efd_name, *args, **kwargs)


class EfdClientTools:
    """Shared class for async and sync EfdClient to avoid duplication code

    Parameters
    ----------
    efd_name : `str`
        Name of the EFD instance for which to retrieve credentials.
    db_name : `str`, optional
        Name of the database within influxDB to query ('efd' by default).
    creds_service : `str`, optional
        URL to the service to retrieve credentials
        (``https://roundtable.lsst.codes/segwarides/`` by default).
    timeout : `int`, optional
        Timeout in seconds for async requests (`aiohttp.ClientSession`). The
        default timeout is 900 seconds.
    client : `object`, optional
        An instance of a class that ducktypes as
        `aioinflux.client.InfluxDBClient`. The intent is to be able to
        substitute a mocked client for testing.
    """

    @staticmethod
    def get_client(
        efd_name,
        mode,
        db_name="efd",
        creds_service="https://roundtable.lsst.codes/segwarides/",
        timeout=900,
        client=None,
    ):
        auth = NotebookAuth(service_endpoint=creds_service)
        (
            host,
            schema_registry_url,
            port,
            user,
            password,
            path,
        ) = auth.get_auth(efd_name)

        if client is None:
            health_url = urljoin(f"https://{host}:{port}", f"{path}health")
            response = requests.get(health_url)
            if response.status_code != 200:
                raise RuntimeError(
                    f"InfluxDB server is not ready. "
                    f"Received code:{response.status_code} "
                    f"when reaching {health_url}."
                )
            client = aioinflux.InfluxDBClient(
                host=host,
                path=path,
                port=port,
                ssl=True,
                username=user,
                password=password,
                db=db_name,
                mode=mode.value,
                output="dataframe",
                timeout=timeout,
            )
        return schema_registry_url, client

    @staticmethod
    def build_time_range_query(
        topic_name,
        fields,
        start,
        end,
        db_name,
        is_window=False,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        # A helper method to buuild the time range query
        if not isinstance(start, Time):
            raise TypeError("The first time argument must be a time stamp")

        if convert_influx_index:
            # Implies index is in TAI, so query should be in TAI
            start = start.tai
        else:
            start = start.utc

        if isinstance(end, TimeDelta):
            if is_window:
                start_str = (start - end / 2).isot
                end_str = (start + end / 2).isot
            else:
                start_str = start.isot
                end_str = (start + end).isot
        elif isinstance(end, Time):
            if convert_influx_index:
                end = end.tai
            else:
                end = end.utc
            start_str = start.isot
            end_str = end.isot
        else:
            raise TypeError(
                "The second time argument must be the time stamp for the end "
                "or a time delta."
            )

        index_str = ""
        if index:
            if use_old_csc_indexing:
                parts = topic_name.split(".")
                index_name = (
                    f"{parts[-2]}ID"  # The CSC name is always the penultimate
                )
            else:
                index_name = "salIndex"
            index_str = f" AND {index_name} = {index}"
        timespan = (
            # influxdb requires the time to be in UTC (Z)
            f"time >= '{start_str}Z' AND time <= '{end_str}Z'{index_str}"
        )

        if isinstance(fields, str):
            fields = [
                fields,
            ]
        elif isinstance(fields, bytes):
            fields = fields.decode()
            fields = [
                fields,
            ]

        # Build query here
        return (
            f'SELECT {", ".join(fields)} FROM "{db_name}"."autogen".'
            f'"{topic_name}" WHERE {timespan}'
        )

    @staticmethod
    def build_select_top_n_query(
        topic_name,
        fields,
        num,
        db_name,
        time_cut=None,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        # A helper method to build the top n query

        # The "GROUP BY" is necessary to return the tags
        limit = f"GROUP BY * ORDER BY DESC LIMIT {num}"
        # Deal with time cut and index
        pstr = ""
        if time_cut:
            pstr = f" WHERE time < '{time_cut}Z'"
        if index:
            if use_old_csc_indexing:
                parts = topic_name.split(".")
                index_name = (
                    f"{parts[-2]}ID"  # The CSC name is always the penultimate
                )
            else:
                index_name = "salIndex"
            # The CSC name is always the penultimate
            istr = f"{index_name} = {index}"
            if pstr != "":
                pstr += f" AND {istr}"
            else:
                pstr = f" WHERE {istr}"

        if isinstance(fields, str):
            fields = [
                fields,
            ]
        elif isinstance(fields, bytes):
            fields = fields.decode()
            fields = [
                fields,
            ]

        # Build query here
        query = (
            f'SELECT {", ".join(fields)} FROM "{db_name}"."autogen".'
            f'"{topic_name}"{pstr} {limit}'
        )
        return query

    @staticmethod
    def _make_fields(fields, base_fields):
        # A helper method to construct the list of fields for a field that

        ret = {}
        n = None
        for bfield in base_fields:
            for f in fields:
                if (
                    f.startswith(bfield) and f[len(bfield) :].isdigit()
                ):  # Check prefix is complete
                    ret.setdefault(bfield, []).append(f)
            if n is None:
                n = len(ret[bfield])
            if n != len(ret[bfield]):
                raise ValueError(
                    f"Field lengths do not agree for {bfield}: {n} vs. "
                    f"{len(ret[bfield])}"
                )

            def sorter(prefix, val):
                return int(val[len(prefix) :])

            part = partial(sorter, bfield)
            ret[bfield].sort(key=part)
        return ret, n

    @classmethod
    def make_fields(cls, fields: str, base_fields: [str, bytes]):
        # A helper method to construct the list of fields for a field that
        if isinstance(base_fields, str):
            base_fields = [
                base_fields,
            ]
        elif isinstance(base_fields, bytes):
            base_fields = base_fields.decode()
            base_fields = [
                base_fields,
            ]
        qfields, els = cls._make_fields(fields, base_fields)
        field_list = []
        for k in qfields:
            field_list += qfields[k]
        return field_list

    @staticmethod
    def merge_packed_time_series(
        result,
        base_fields,
        ref_timestamp_col="cRIO_timestamp",
        ref_timestamp_fmt="unix_tai",
        ref_timestamp_scale="tai",
    ):
        # A helper method to merge packed time series
        vals = {}
        for f in base_fields:
            df = merge_packed_time_series(
                result,
                f,
                ref_timestamp_col=ref_timestamp_col,
                fmt=ref_timestamp_fmt,
                scale=ref_timestamp_scale,
            )
            vals[f] = df[f]
        vals.update({"times": df["times"]})
        return pd.DataFrame(vals, index=df.index)

    @staticmethod
    def parse_schema(topic, schema):
        # A helper function so we can test our parsing
        fields = schema["schema"]["fields"]
        vals = {
            "name": [],
            "description": [],
            "units": [],
            "aunits": [],
            "is_array": [],
        }
        for f in fields:
            vals["name"].append(f["name"])
            if "description" in f:
                vals["description"].append(f["description"])
            else:
                vals["description"].append(None)
            if "units" in f:
                vals["units"].append(f["units"])
                # Special case not having units
                if (
                    vals["units"][-1] == "unitless"
                    or vals["units"][-1] == "dimensionless"
                ):
                    vals["aunits"].append(u.dimensionless_unscaled)
                else:
                    vals["aunits"].append(u.Unit(vals["units"][-1]))
            else:
                vals["units"].append(None)
                vals["aunits"].append(None)
            if isinstance(f["type"], dict) and f["type"]["type"] == "array":
                vals["is_array"].append(True)
            else:
                vals["is_array"].append(False)
        return pd.DataFrame(vals)

    @staticmethod
    def handle_query_result(query_result: Any, convert_influx_index=False):
        """Sets query_result to empty Dataframe if its not a valid dataframe
        or None, and set dataframe index to time if convert_influx_index
        is True.

        Parameters
        ----------
        query_result: `~pandas.DataFrame`.
            Pandas dataframe
        convert_influx_index: `bool`
            If True sets dataframe index as a new Time column

        Result
        ------
        dataframe: `pandas.Dataframe`
            Query result modified dataframe

        """
        if not isinstance(query_result, pd.DataFrame) and not query_result:
            # aioinflux returns an empty dict for an empty query
            query_result = pd.DataFrame()
        elif convert_influx_index:
            times = Time(query_result.index, format="datetime", scale="tai")
            query_result = query_result.set_index(times.utc.datetime)
        return query_result

    @staticmethod
    def filter_topics(topics, to_find, case_sensitive=False):
        """Return all the strings in topics which match
        the query string.

        Supports wildcards, which are denoted as `*``, as per shell globs.

        Example:
        >>> # assume topics are ['apple', 'banana', 'grape']
        >>> getTopics(, 'a*p*')
        ['apple', 'grape']

        Parameters
        ----------
        to_find : `str`, optional
            The query string, with optional wildcards denoted as *.
        case_sensitive : `bool`, optional
            If ``True``, the query is case sensitive. Defaults to ``False``.

        Returns
        -------
        matches : `list` of `str`
            The list of matching topics.
        """
        # Replace wildcard with regex equivalent
        pattern = to_find.replace("*", ".*")
        flags = re.IGNORECASE if not case_sensitive else 0

        matches = []
        for topic in topics:
            if re.match(pattern, topic, flags):
                matches.append(topic)
        return matches

    @staticmethod
    def get_command_times(data, command, time_format="pandas"):
        # Helper function to get the command times
        command_times = {}  # type: ignore
        for time, _ in data.iterrows():
            # this is much the most simple data structure, and the chance
            # of commands being *exactly* simultaneous is minimal so try
            # it like this, and just raise if we get collisions for now. So
            # far in testing this seems to be just fine.

            time_key = None
            match time_format:
                case "pandas":
                    time_key = time
                case "astropy":
                    time_key = Time(time)
                case "python":
                    time_key = time.to_pydatetime()

            if time_key in command_times:
                raise ValueError(
                    f"There is already a command at {time_key=} -"
                    " make a better data structure!"
                )
            command_times[time_key] = command
        return command_times


class EfdClientSync(_EfdClientStatic):
    """Class to handle connections and basic queries synchronously

    Parameters
    ----------
    efd_name : `str`
        Name of the EFD instance for which to retrieve credentials.
    db_name : `str`, optional
        Name of the database within influxDB to query ('efd' by default).
    creds_service : `str`, optional
        URL to the service to retrieve credentials
        (``https://roundtable.lsst.codes/segwarides/`` by default).
    timeout : `int`, optional
        Timeout in seconds for async requests (`aiohttp.ClientSession`). The
        default timeout is 900 seconds.
    client : `object`, optional
        An instance of a class that ducktypes as
        `aioinflux.client.InfluxDBClient`. The intent is to be able to
        substitute a mocked client for testing.
    """

    mode = ClientMode.SYNC

    def __init__(
        self,
        efd_name,
        db_name="efd",
        creds_service="https://roundtable.lsst.codes/segwarides/",
        timeout=900,
        client=None,
    ):
        (
            self._schema_registry_url,
            self._influx_client,
        ) = EfdClientTools.get_client(
            efd_name,
            EfdClientSync.mode,
            db_name,
            creds_service,
            timeout,
            client,
        )
        self._db_name = db_name
        self._query_history = []

    def _do_query(self, query: str, convert_influx_index=False):
        """Query the influxDB and return results

        Parameters
        ----------
        query : `str`
            Query string to execute.
        convert_influx_index : `bool`
            Legacy flag to convert time index from TAI to UTC

        Returns
        -------
        results : `pandas.DataFrame`
            Results of the query in a `~pandas.DataFrame`.
        """
        self._query_history.append(query)
        result = self._influx_client.query(query)
        return EfdClientTools.handle_query_result(
            result, convert_influx_index=convert_influx_index
        )

    def _get_topics(self):
        """Query the list of possible topics.

        Returns
        -------
        results : `list`
            List of valid topics in the database.
        """
        topics = self._do_query("SHOW MEASUREMENTS")
        return topics["name"].tolist()

    def get_topics(self, to_find=None, case_sensitive=False):
        """Query the list of possible topics.
        List can be filtered to return all the strings in topics which match
        the topic query string if to_find argument is given.

        Supports wildcards, which are denoted as '*', as per shell globs.

        Example:
        >>> # assume topics are ['apple', 'banana', 'grape']
        >>> getTopics(, 'a*p*')
        ['apple', 'grape']

        Parameters
        ----------
        to_find : `str`, optional
            The query string, with optional wildcards denoted as '*'.
        case_sensitive : `bool`, optional
            If ``True``, the query is case sensitive. Defaults to ``False``.

        Returns
        -------
        matches : `list` of `str`
            The list of matching topics.
        """
        topics = self._get_topics()
        if to_find is None:
            return topics
        return EfdClientTools.filter_topics(topics, to_find, case_sensitive)

    def get_fields(self, topic_name):
        """Query the list of field names for a topic.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query for field names.

        Returns
        -------
        results : `list`
            List of field names in specified topic.
        """
        fields = self._do_query(
            f'SHOW FIELD KEYS FROM "{self._db_name}"'
            f'."autogen"."{topic_name}"'
        )
        return fields["fieldKey"].tolist()

    @property
    def query_history(self):
        """Return query history

        Returns
        -------
        results : `list`
            All queries made with this client instance
        """
        return self._query_history

    def build_time_range_query(
        self,
        topic_name,
        fields,
        start,
        end,
        is_window=False,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Build a query based on a time range.

        Parameters
        ----------
        topic_name : `str`
            Name of topic for which to build a query.
        fields :  `str` or `list`
            Name of field(s) to query.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        query : `str`
            A string containing the constructed query statement.
        """
        return EfdClientTools.build_time_range_query(
            topic_name,
            fields,
            start,
            end,
            self._db_name,
            is_window,
            index,
            convert_influx_index,
            use_old_csc_indexing,
        )

    def select_time_series(
        self,
        topic_name,
        fields,
        start,
        end,
        is_window=False,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select a time series for a set of topics in a single subsystem

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        fields :  `str` or `list`
            Name of field(s) to query.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        query = self.build_time_range_query(
            topic_name,
            fields,
            start,
            end,
            is_window,
            index,
            convert_influx_index,
            use_old_csc_indexing,
        )
        ret = self._do_query(query, convert_influx_index)
        if ret.empty and not self._is_topic_valid(topic_name):
            raise ValueError(f"Topic {topic_name} not in EFD schema")
        return ret

    def select_top_n(
        self,
        topic_name,
        fields,
        num,
        time_cut=None,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select the most recent N samples from a set of topics in a single
        subsystem.

        This method does not guarantee sort direction of the returned rows.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        fields : `str` or `list`
            Name of field(s) to query.
        num : `int`
            Number of rows to return.
        time_cut : `astropy.time.Time`, optional
            Use a time cut instead of the most recent entry.
            (default is `None`)
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        query = EfdClientTools.build_select_top_n_query(
            topic_name,
            fields,
            num,
            self._db_name,
            time_cut=None,
            index=None,
            convert_influx_index=False,
            use_old_csc_indexing=False,
        )

        ret = self._do_query(query, convert_influx_index)

        if ret.empty and not self._is_topic_valid(topic_name):
            raise ValueError(f"Topic {topic_name} not in EFD schema")
        return ret

    def select_packed_time_series(
        self,
        topic_name,
        base_fields,
        start,
        end,
        is_window=False,
        index=None,
        ref_timestamp_col="cRIO_timestamp",
        ref_timestamp_fmt="unix_tai",
        ref_timestamp_scale="tai",
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select fields that are time samples and unpack them into a
        dataframe.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        base_fields :  `str` or `list`
            Base field name(s) that will be expanded to query all
            vector entries.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        ref_timestamp_col : `str`, optional
            Name of the field name to use to assign timestamps to unpacked
            vector fields (default is 'cRIO_timestamp').
        ref_timestamp_fmt : `str`, optional
            Format to use to translating ``ref_timestamp_col`` values
            (default is 'unix_tai').
        ref_timestamp_scale : `str`, optional
            Time scale to use in translating ``ref_timestamp_col`` values
            (default is 'tai').
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        fields = self.get_fields(topic_name)
        field_list = EfdClientTools.make_fields(fields, base_fields)
        result = self.select_time_series(
            topic_name,
            field_list
            + [
                ref_timestamp_col,
            ],
            start,
            end,
            is_window=is_window,
            index=index,
            convert_influx_index=convert_influx_index,
            use_old_csc_indexing=use_old_csc_indexing,
        )
        return EfdClientTools.merge_packed_time_series(
            result,
            base_fields,
            ref_timestamp_col,
            ref_timestamp_fmt,
            ref_timestamp_scale,
        )

    def _is_topic_valid(self, topic: str) -> bool:
        # A helper function that check if the specified topic is in the schema.
        # A topic is valid and returns `True` if it is in the cached list of
        # topics. Any other case returns `False`.
        existing_topics = self._get_topics()
        return topic in existing_topics

    def get_efd_data(
        self,
        topic,
        *,
        columns=None,
        pre_padding=0,
        post_padding=0,
        day_obs=None,
        begin=None,
        end=None,
        timespan=None,
        event=None,
        exp_record=None,
        warn=True,
    ):
        """Get one or more EFD topics over a time range, synchronously.

        The time range can be specified as either:
            * a dayObs, in which case the full 24 hour period is used,
            * a begin point and a end point,
            * a begin point and a timespan.
            * a mount event
            * an exposure record
        If it is desired to use an end time with a timespan, just specify
        it as the begin time and use a negative timespan.

        The results from all topics are merged into a single dataframe.

        Parameters
        ----------
        topic : `str`
            The topic to query.
        columns : `list` of `str`, optional
            The columns to query. If not specified, all columns are queried.
        pre_padding : `float`
            The amount of time before the nominal start of the query to
            include, in seconds.
        post_padding : `float`
            The amount of extra time after the nominal end of the query to
            include, in seconds.
        day_obs : `int`, optional
            The dayObs to query. If specified, this is used to determine
            the begin and end times.
        begin : `astropy.Time`, optional
            The begin time for the query. If specified, either a end time or a
            timespan must be supplied.
        end : `astropy.time.Time`, optional
            The end time for the query. If specified, a begin time must also be
            supplied.
        timespan : `astropy.time.TimeDelta`, optional
            The timespan for the query. If specified, a begin time must also be
            supplied.
        event : lsst.summit.utils.efdUtils.TmaEvent, optional
            The event to query. If specified, this is used to determine
            the begin and end times, and all other options are disallowed.
        exp_record : lsst.daf.butler.dimensions.DimensionRecord, optional
            The exposure record containing the timespan to query. If specified,
            all other options are disallowed.
        warn : bool, optional
            If ``True``, warn when no data is found. Exists so that
            utility code can disable warnings when checking for data,
            and therefore defaults to ``True``.

        Returns
        -------
        data : `pandas.DataFrame`
            The merged data from all topics.

        Raises
        ------
        ValueError:
            If the topics are not in the EFD schema.
        ValueError:
            If both a dayObs and a begin/end or timespan are specified.
        ValueError:
            If a begin time is specified but no end time or timespan.

        """
        begin, end = get_begin_end(
            day_obs, begin, end, timespan, event, exp_record
        )
        begin -= TimeDelta(pre_padding, format="sec")
        end += TimeDelta(post_padding, format="sec")

        if columns is None:
            columns = ["*"]
        columns = list(ensure_iterable(columns))

        available_topics = self.get_topics()

        if topic not in available_topics:
            raise ValueError(f"Topic {topic} not in EFD schema")

        data = self.select_time_series(topic, columns, begin.utc, end.utc)

        if data.empty and warn:
            raise Exception(
                f"Topic {topic} is in the schema, but no data was returned "
                "by the query for the specified time range"
            )
        return data

    def get_most_recent_row_with_data_before(
        self, topic, time_to_look_before, warn_stale_after_N_minutes=60 * 12
    ):
        """Get the most recent row of data for a topic before a given time.

        Parameters
        ----------
        topic : `str`
            The topic to query.
        time_to_look_before : `astropy.time.Time`
            The time to look before.
        warn_stale_after_N_minutes : `float`, optional
            The number of minutes after which to consider the
            data stale and issue a warning.

        Returns
        -------
        row : `pandas.Series`
            The row of data from the EFD containing the most
            recent data before the specified time.

        Raises
        ------
        ValueError:
            If the topic is not in the EFD schema.
        """
        stale_age = datetime.timedelta(warn_stale_after_N_minutes)

        first_day_possible = get_day_obs_start_time(20190101)

        if time_to_look_before < first_day_possible:
            raise ValueError(
                f"Requested time {time_to_look_before} is before any data"
                f"was put in the EFD"
            )

        df = pd.DataFrame()
        begin_time = time_to_look_before
        while df.empty and begin_time > first_day_possible:
            df = self.get_efd_data(
                topic, begin=begin_time, timespan=-TIME_CHUNKING, warn=False
            )
            begin_time -= TIME_CHUNKING

        if (
            begin_time < first_day_possible and df.empty
        ):  # we ran all the way back to the beginning of time
            raise ValueError(
                f"The entire EFD was searched backwards from"
                f"{time_to_look_before}"
                f"and no data was found in {topic=}"
            )

        last_row = df.iloc[-1]
        command_time = efd_timestamp_to_astropy(last_row["private_efdStamp"])

        command_age = time_to_look_before - command_time
        if command_age > stale_age:
            log = logging.getLogger(__name__)
            log.warning(
                f"Component {topic} was last set"
                f"{command_age.sec/60:.1} minutes"
                f" before the requested time"
            )

        return last_row

    def get_commands(
        self,
        commands,
        begin,
        end,
        pre_padding,
        post_padding,
        time_format,
    ):
        """Retrieve the commands issued within a
        specified time range.

        Parameters
        ----------
        commands : `list`
            A list of commands to retrieve.
        begin : `astropy.time.Time`
            The start time of the time range.
        end : `astropy.time.Time`
            The end time of the time range.
        pre_padding : `float`
            The amount of time to pad before the begin time.
        post_padding : `float`
            The amount of time to pad after the end time.
        time_format : `str`
            One of 'pandas' or 'astropy' or 'python'. If 'pandas',
            the dictionary keys will be pandas timestamps, if 'astropy'
            they will be astropy times and if 'python' they will be
            python datetimes.

        Returns
        -------
        command_times : `dict` [`time`, `str`]
            A dictionary of the times at which the commands where issued.
            The type that `time` takes is determined by the format key,
            and defaults to python datetime.

        Raises
        ------
        ValueError
            Raise if there is already a command at a timestamp
            in the dictionary,
            i.e. there is a collision.
        """
        check_time_format(time_format)
        commands = list(ensure_iterable(commands))

        for command in commands:
            data = self.get_efd_data(
                command,
                begin=begin,
                end=end,
                pre_padding=pre_padding,
                post_padding=post_padding,
                warn=False,  # most commands will not be issue so we expect many empty queries  # noqa: E501
            )
            command_times = EfdClientTools.get_command_times(
                data, command, time_format
            )
        return command_times

    def get_schema(self, topic):
        """
        Given a topic, get a list of dictionaries describing the fields

        Parameters
        ----------
        topic : `str`
            The name of the topic to query. A full list of valid topic names
            can be obtained using ``get_schema_topics``.

        Returns
        -------
        result : `pandas.DataFrame`
            A dataframe with the schema information for the topic.
            One row per field.
        """
        with requests.Session() as http_session:
            http_session.trust_env = False
            registry_api = SyncSchemaParser(
                session=http_session,
                url=self._schema_registry_url,
            )
            schema = registry_api.get_schema_by_subject(f"{topic}-value")
        return EfdClientTools.parse_schema(topic, schema)


class EfdClient(_EfdClientStatic):
    """Class to handle connections and basic queries asynchronously

    Parameters
    ----------
    efd_name : `str`
        Name of the EFD instance for which to retrieve credentials.
    db_name : `str`, optional
        Name of the database within influxDB to query ('efd' by default).
    creds_service : `str`, optional
        URL to the service to retrieve credentials
        (``https://roundtable.lsst.codes/segwarides/`` by default).
    timeout : `int`, optional
        Timeout in seconds for async requests (`aiohttp.ClientSession`). The
        default timeout is 900 seconds.
    client : `object`, optional
        An instance of a class that ducktypes as
        `aioinflux.client.InfluxDBClient`. The intent is to be able to
        substitute a mocked client for testing.
    """

    mode = ClientMode.ASYNC

    def __init__(
        self,
        efd_name,
        db_name="efd",
        creds_service="https://roundtable.lsst.codes/segwarides/",
        timeout=900,
        client=None,
    ):
        (
            self._schema_registry_url,
            self._influx_client,
        ) = EfdClientTools.get_client(
            efd_name, EfdClient.mode, db_name, creds_service, timeout, client
        )
        self._db_name = db_name
        self._query_history = []

    async def _do_query(self, query: str, convert_influx_index=False):
        #  Helper function to do influxdb queries.
        self._query_history.append(query)
        result = await self._influx_client.query(query)
        return EfdClientTools.handle_query_result(
            result, convert_influx_index=convert_influx_index
        )

    async def _get_topics(self):
        #  Query the list of possible topics.
        topics = await self._do_query("SHOW MEASUREMENTS")
        return topics["name"].tolist()

    async def get_topics(self, to_find=None, case_sensitive=False):
        """Query the list of possible topics.
        List can be filtered to return all the strings in topics which match
        the topic query string if to_find argument is given.

        Supports wildcards, which are denoted as '*', as per shell globs.

        Example:
        >>> # assume topics are ['apple', 'banana', 'grape']
        >>> getTopics(, 'a*p*')
        ['apple', 'grape']

        Parameters
        ----------
        to_find : `str`, optional
            The query string, with optional wildcards denoted as '*'.
        case_sensitive : `bool`, optional
            If True, the query is case sensitive. Defaults to False.

        Returns
        -------
        matches : `list` of `str`
            The list of matching topics.
        """
        topics = await self._get_topics()
        if to_find is None:
            return topics
        return EfdClientTools.filter_topics(topics, to_find, case_sensitive)

    async def get_fields(self, topic_name):
        """Query the list of field names for a topic.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query for field names.

        Returns
        -------
        results : `list`
            List of field names in specified topic.
        """
        fields = await self._do_query(
            f'SHOW FIELD KEYS FROM "{self._db_name}"'
            f'."autogen"."{topic_name}"'
        )
        return fields["fieldKey"].tolist()

    @property
    def query_history(self):
        """Return query history

        Returns
        -------
        results : `list`
            All queries made with this client instance
        """
        return self._query_history

    def build_time_range_query(
        self,
        topic_name,
        fields,
        start,
        end,
        is_window=False,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Build a query based on a time range.

        Parameters
        ----------
        topic_name : `str`
            Name of topic for which to build a query.
        fields :  `str` or `list`
            Name of field(s) to query.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        query : `str`
            A string containing the constructed query statement.
        """
        return EfdClientTools.build_time_range_query(
            topic_name,
            fields,
            start,
            end,
            self._db_name,
            is_window,
            index,
            convert_influx_index,
            use_old_csc_indexing,
        )

    async def select_time_series(
        self,
        topic_name,
        fields,
        start,
        end,
        is_window=False,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select a time series for a set of topics in a single subsystem

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        fields :  `str` or `list`
            Name of field(s) to query.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        query = self.build_time_range_query(
            topic_name,
            fields,
            start,
            end,
            is_window,
            index,
            convert_influx_index,
            use_old_csc_indexing,
        )
        ret = await self._do_query(query, convert_influx_index)
        if ret.empty and not await self._is_topic_valid(topic_name):
            raise ValueError(f"Topic {topic_name} not in EFD schema")
        return ret

    async def select_top_n(
        self,
        topic_name,
        fields,
        num,
        time_cut=None,
        index=None,
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select the most recent N samples from a set of topics in a single
        subsystem.

        This method does not guarantee sort direction of the returned rows.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        fields : `str` or `list`
            Name of field(s) to query.
        num : `int`
            Number of rows to return.
        time_cut : `astropy.time.Time`, optional
            Use a time cut instead of the most recent entry.
            (default is `None`)
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        query = EfdClientTools.build_select_top_n_query(
            topic_name,
            fields,
            num,
            self._db_name,
            time_cut=None,
            index=None,
            convert_influx_index=False,
            use_old_csc_indexing=False,
        )

        ret = await self._do_query(query, convert_influx_index)

        if ret.empty and not await self._is_topic_valid(topic_name):
            raise ValueError(f"Topic {topic_name} not in EFD schema")
        return ret

    async def select_packed_time_series(
        self,
        topic_name,
        base_fields,
        start,
        end,
        is_window=False,
        index=None,
        ref_timestamp_col="cRIO_timestamp",
        ref_timestamp_fmt="unix_tai",
        ref_timestamp_scale="tai",
        convert_influx_index=False,
        use_old_csc_indexing=False,
    ):
        """Select fields that are time samples and unpack them into a
        dataframe.

        Parameters
        ----------
        topic_name : `str`
            Name of topic to query.
        base_fields :  `str` or `list`
            Base field name(s) that will be expanded to query all
            vector entries.
        start : `astropy.time.Time`
            Start time of the time range, if ``is_window`` is specified,
            this will be the midpoint of the range.
        end : `astropy.time.Time` or `astropy.time.TimeDelta`
            End time of the range either as an absolute time or
            a time offset from the start time.
        is_window : `bool`, optional
            If set and the end time is specified as a
            `~astropy.time.TimeDelta`, compute a range centered on the start
            time (default is `False`).
        index : `int`, optional
            When index is used, add an 'AND salIndex = index' to the query.
            (default is `None`).
        ref_timestamp_col : `str`, optional
            Name of the field name to use to assign timestamps to unpacked
            vector fields (default is 'cRIO_timestamp').
        ref_timestamp_fmt : `str`, optional
            Format to use to translating ``ref_timestamp_col`` values
            (default is 'unix_tai').
        ref_timestamp_scale : `str`, optional
            Time scale to use in translating ``ref_timestamp_col`` values
            (default is 'tai').
        convert_influx_index : `bool`, optional
            Convert influxDB time index from TAI to UTC? This is for legacy
            instances that may still have timestamps stored internally as TAI.
            Modern instances all store index timestamps as UTC natively.
            Default is `False`, don't translate from TAI to UTC.
        use_old_csc_indexing: `bool`, optional
            When index is used, add an 'AND {CSCName}ID = index' to the query
            which is the old CSC indexing name.
            (default is `False`).

        Returns
        -------
        result : `pandas.DataFrame`
            A `~pandas.DataFrame` containing the results of the query.
        """
        fields = await self.get_fields(topic_name)
        field_list = EfdClientTools.make_fields(fields, base_fields)
        result = await self.select_time_series(
            topic_name,
            field_list
            + [
                ref_timestamp_col,
            ],
            start,
            end,
            is_window=is_window,
            index=index,
            convert_influx_index=convert_influx_index,
            use_old_csc_indexing=use_old_csc_indexing,
        )
        return EfdClientTools.merge_packed_time_series(
            result,
            base_fields,
            ref_timestamp_col,
            ref_timestamp_fmt,
            ref_timestamp_scale,
        )

    async def _is_topic_valid(self, topic: str) -> bool:
        # A helper function that check if the specified topic is in the schema.
        # A topic is valid and returns `True` if it is in the cached list of
        # topics. Any other case returns `False`.
        existing_topics = await self._get_topics()
        return topic in existing_topics

    async def get_efd_data(
        self,
        topic,
        *,
        columns=None,
        pre_padding=0,
        post_padding=0,
        day_obs=None,
        begin=None,
        end=None,
        timespan=None,
        event=None,
        exp_record=None,
        warn=True,
    ):
        """Get one or more EFD topics over a time range, synchronously.

        The time range can be specified as either:
            * a dayObs, in which case the full 24 hour period is used,
            * a begin point and a end point,
            * a begin point and a timespan.
            * a mount event
            * an exposure record
        If it is desired to use an end time with a timespan, just specify it
        as the begin time and use a negative timespan.

        The results from all topics are merged into a single dataframe.

        Parameters
        ----------
        topic : `str`
            The topic to query.
        columns : `list` of `str`, optional
            The columns to query. If not specified, all columns are queried.
        pre_padding : `float`
            The amount of time before the nominal start of the query to
            include, in seconds.
        post_padding : `float`
            The amount of extra time after the nominal end of the query to
            include, in seconds.
        day_obs : `int`, optional
            The dayObs to query. If specified, this is used to determine the
            begin and end times.
        begin : `astropy.time.Time`, optional
            The begin time for the query. If specified, either a end time or a
            timespan must be supplied.
        end : `astropy.time.Time`, optional
            The end time for the query. If specified, a begin time must also be
            supplied.
        timespan : `astropy.time.TimeDelta`, optional
            The timespan for the query. If specified, a begin time must also be
            supplied.
        event : lsst.summit.utils.efdUtils.TmaEvent, optional
            The event to query. If specified, this is used to determine the
            begin and end times, and all other options are disallowed.
        exp_record : lsst.daf.butler.dimensions.DimensionRecord, optional
            The exposure record containing the timespan to query. If specified
            all other options are disallowed.
        warn : bool, optional
            If ``True``, warn when no data is found. Exists so that utility
            code can disable warnings when checking for data, and therefore
            defaults to ``True``.

        Returns
        -------
        data : `pandas.DataFrame`
            The merged data from all topics.

        Raises
        ------
        ValueError:
            If the topics are not in the EFD schema.
        ValueError:
            If both a dayObs and a begin/end or timespan are specified.
        ValueError:
            If a begin time is specified but no end time or timespan.

        """
        begin, end = get_begin_end(
            day_obs, begin, end, timespan, event, exp_record
        )
        begin -= TimeDelta(pre_padding, format="sec")
        end += TimeDelta(post_padding, format="sec")

        if columns is None:
            columns = ["*"]
        columns = list(ensure_iterable(columns))

        available_topics = await self.get_topics()

        if topic not in available_topics:
            raise ValueError(f"Topic {topic} not in EFD schema")

        data = await self.select_time_series(
            topic, columns, begin.utc, end.utc
        )

        if data.empty and warn:
            raise Exception(
                f"Topic {topic} is in the schema, but no data was returned "
                "by the query for the specified time range"
            )
        return data

    async def get_most_recent_row_with_data_before(
        self, topic, time_to_look_before, warn_stale_after_N_minutes
    ):
        """Get the most recent row of data for a topic before a given time.

        Parameters
        ----------
        topic : `str`
            The topic to query.
        time_to_look_before : `astropy.time.Time`
            The time to look before.
        warn_stale_after_N_minutes : `float`, optional
            The number of minutes after which to consider the data
            stale and issue a warning.

        Returns
        -------
        row : `pandas.Series`
            The row of data from the EFD containing the most recent data
            before the specified time.

        Raises
        ------
        ValueError:
            If the topic is not in the EFD schema.
        """
        stale_age = datetime.timedelta(warn_stale_after_N_minutes)

        first_day_possible = get_day_obs_start_time(20190101)

        if time_to_look_before < first_day_possible:
            raise ValueError(
                f"Requested time {time_to_look_before}"
                f"is before any data was put in the EFD"
            )

        df = pd.DataFrame()
        begin_time = time_to_look_before
        while df.empty and begin_time > first_day_possible:
            df = await self.get_efd_data(
                topic, begin=begin_time, timespan=-TIME_CHUNKING, warn=False
            )
            begin_time -= TIME_CHUNKING

        if (
            begin_time < first_day_possible and df.empty
        ):  # we ran all the way back to the beginning of time
            raise ValueError(
                f"The entire EFD was searched backwards from"
                f"{time_to_look_before} and no data was found in {topic=}"
            )

        last_row = df.iloc[-1]
        command_time = efd_timestamp_to_astropy(last_row["private_efdStamp"])

        command_age = time_to_look_before - command_time
        if command_age > stale_age:
            log = logging.getLogger(__name__)
            log.warning(
                f"Component {topic} was last set {command_age.sec/60:.1}"
                "minutes before the requested time"
            )

        return last_row

    async def get_commands(
        self,
        commands,
        begin,
        end,
        pre_padding,
        post_padding,
        time_format,
    ):
        """Retrieve the commands issued within a specified time range.

        Parameters
        ----------
        commands : `list`
            A list of commands to retrieve.
        begin : `astropy.time.Time`
            The start time of the time range.
        end : `astropy.time.Time`
            The end time of the time range.
        pre_padding : `float`
            The amount of time to pad before the begin time.
        post_padding : `float`
            The amount of time to pad after the end time.
        time_format : `str`
            One of 'pandas' or 'astropy' or 'python'. If 'pandas',
            the dictionary keys will be pandas timestamps, if 'astropy'
            they will be astropy times and if 'python' they will be
            python datetimes.

        Returns
        -------
        command_times : `dict` [`time`, `str`]
            A dictionary of the times at which the commands where issued.
            The type that `time` takes is determined by the format key,
            and defaults to python datetime.

        Raises
        ------
        ValueError
            Raise if there is already a command at a timestamp
            in the dictionary,
            i.e. there is a collision.
        """
        check_time_format(time_format)
        commands = list(ensure_iterable(commands))

        for command in commands:
            data = await self.get_efd_data(
                command,
                begin=begin,
                end=end,
                pre_padding=pre_padding,
                post_padding=post_padding,
                warn=False,  # most commands will not be issue so we expect many empty queries  # noqa: E501
            )
            command_times = EfdClientTools.get_command_times(
                data, command, time_format
            )
        return command_times

    async def get_schema(self, topic):
        """
        Given a topic, get a list of dictionaries describing the fields

        Parameters
        ----------
        topic : `str`
            The name of the topic to query. A full list of valid topic names
            can be obtained using ``get_schema_topics``.

        Returns
        -------
        result : `pandas.DataFrame`
            A dataframe with the schema information for the topic.
            One row per field.
        """
        async with aiohttp.ClientSession() as http_session:
            registry_api = RegistryApi(
                session=http_session,
                url=self._schema_registry_url,
            )
            schema = await registry_api.get_schema_by_subject(f"{topic}-value")
        return EfdClientTools.parse_schema(topic, schema)
