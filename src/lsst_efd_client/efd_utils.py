"""Free functions to help out with EFD operations."""

import contextlib
import datetime
import json
from collections.abc import Mapping
from typing import Any

import fastavro
import numpy as np
import pandas as pd
import requests
from astropy import units as u
from astropy.time import Time, TimeDelta
from kafkit.httputils import format_url
from kafkit.registry.sansio import (
    SchemaCache,
    SubjectCache,
    decipher_response,
    make_headers,
)


def merge_packed_time_series(
    packed_dataframe,
    base_field,
    stride=1,
    ref_timestamp_col="cRIO_timestamp",
    fmt="unix_tai",
    scale="tai",
):
    """Select fields that are time samples and unpack them into a dataframe.

    Parameters
    ----------
    packed_dataframe : `pandas.DataFrame`
        packed data frame containing the desired data
    base_field :  `str`
        Base field name that will be expanded to query all
        vector entries.
    stride : `int`, optional
        Only use every stride value when unpacking. Must be a factor
        of the number of packed values. (1 by default)
    ref_timestamp_col : `str`, optional
        Name of the field name to use to assign timestamps to unpacked
        vector fields (default is 'cRIO_timestamp').
    fmt : `str`, optional
        Format to give to the `astropy.time.Time` constructor. Defaults to
        'unix_tai' since most internal timestamp columns are in TAI.
    scale : `str`, optional
        Time scale to give to the `astropy.time.Time` constructor. Defaults to
        'tai'.

    Returns
    -------
    result : `pandas.DataFrame`
        A `pandas.DataFrame` containing the results of the query.
    """

    packed_fields = [
        k
        for k in packed_dataframe.keys()
        if k.startswith(base_field) and k[len(base_field) :].isdigit()
    ]
    packed_fields = sorted(
        packed_fields, key=lambda k: int(k[len(base_field) :])
    )  # sort by pack ID
    npack = len(packed_fields)
    if npack % stride != 0:
        raise RuntimeError(
            "Stride must be a factor of the number of packed fields: "
            f"{stride} v. {npack}"
        )
    packed_len = len(packed_dataframe)
    n_used = npack // stride  # number of raw fields being used
    output = np.empty(n_used * packed_len)
    times = np.empty_like(output, dtype=packed_dataframe[ref_timestamp_col][0])

    if packed_len == 1:
        dt = 0
    else:
        dt = (
            packed_dataframe[ref_timestamp_col][1]
            - packed_dataframe[ref_timestamp_col][0]
        ) / npack
    for i in range(0, npack, stride):
        i0 = i // stride
        output[i0::n_used] = packed_dataframe[f"{base_field}{i}"]
        times[i0::n_used] = packed_dataframe[ref_timestamp_col] + i * dt

    timestamps = Time(times, format=fmt, scale=scale)
    return pd.DataFrame(
        {base_field: output, "times": times}, index=timestamps.utc.datetime64
    )


def resample(df1, df2, interp_type="time"):
    """Resample one DataFrame onto another.

    Parameters
    ----------
    df1 : `pandas.DataFrame`
        First `~pandas.DataFrame`.
    df2 : `pandas.DataFrame`
        Second `~pandas.DataFrame`.
    interp_type : `str`, optional
        Type of interpolation to perform (default is 'time').

    Returns
    -------
    result : `pandas.DataFrame`
        The resulting resampling is bi-directional.
        That is the length of the resulting `~pandas.DataFrame` is the
        sum of the lengths of the inputs.
    """
    df = pd.concat(
        [df1, df2], axis=1
    )  # Sort in this context does not sort the data
    df = df.sort_index()
    return df.interpolate(type=interp_type)


def rendezvous_dataframes(
    left,
    right,
    direction="backward",
    tolerance=pd.Timedelta(days=20),
    **kwargs,
):
    """Extend each record in ``left`` with a corresponding record in "right",
    if one exists.

    By default, the record in ``right`` will be the most recent record
    in the past. The other options are the closest record in the future
    and the nearest overall.

    Parameters
    ----------
    left: `pandas.DataFrame`
        The `~pandas.DataFrame` to extend
    right: `pandas.DataFrame`
        The `~pandas.DataFrame` to rendezvous with ``left``
    direction: `str`
        The direction to search for the nearest record. Default is
        ``backward``. The other options are ``forward`` and ``nearest``.
    tolerance: `pandas.Timedelta`
        The to,e window to search for the matching record.
    kwargs: `dict`
        Additional keyword arguments will be forwarded to the
        `pandas.merge_asof` function.
    """
    return pd.merge_asof(
        left,
        right,
        left_index=True,
        right_index=True,
        tolerance=tolerance,
        direction=direction,
        **kwargs,
    )


class SyncSchemaParser:
    """
    Class based on `~kafkit.registry.sansio.RegistryApi` to be able
    to parse the Confluence schema synchronously
    """

    def __init__(self, session: requests.Session, url: str):
        self._session = session
        self._url = url
        self._schema_cache = SchemaCache()
        self._subject_cache = SubjectCache(self._schema_cache)

    @property
    def schema_cache(self) -> SchemaCache:
        """The schema cache (`~kafkit.registry.sansio.SchemaCache`)."""
        return self._schema_cache

    @property
    def subject_cache(self) -> SubjectCache:
        """The subject cache (`~kafkit.registry.sansio.SubjectCache`)."""
        return self._subject_cache

    def _request(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes
    ) -> tuple[int, Mapping[str, str], bytes]:
        with self._session.request(
            method, url, headers=headers, data=body
        ) as response:
            return response.status_code, response.headers, response.content

    def _make_request(
        self, method: str, url: str, url_vars: Mapping[str, str], data: Any
    ) -> Any:
        """Construct and make an HTTP request."""
        expanded_url = format_url(host=self._url, url=url, url_vars=url_vars)
        request_headers = make_headers()

        if data == b"":
            body = b""
            request_headers["content-length"] = "0"
        else:
            charset = "utf-8"
            body = json.dumps(data).encode(charset)
            request_headers[
                "content-type"
            ] = f"application/json; charset={charset}"
            request_headers["content-length"] = str(len(body))

        response = self._request(method, expanded_url, request_headers, body)
        return decipher_response(*response)

    def get(self, url: str, url_vars: Mapping[str, str] | None = None) -> Any:
        """Send an HTTP GET request.

        Parameters
        ----------
        url : `str`
            The endpoint path, usually relative to the ``RegistryApi.url``
            attribute (an absolute URL is also okay). The url can be templated
            (``/a{/b}/c``, where ``b`` is a variable).
        url_vars : `dict`, optional
            A dictionary of variable names and values to expand the templated
            ``url`` parameter.

        Returns
        -------
        data : bytes
            The response body. If the response is JSON, the data is parsed
            into a Python object.

        Raises
        ------
        kafkit.registry.RegistryRedirectionError
            Raised if the server returns a 3XX status.
        kafkit.registry.RegistryBadRequestError
            Raised if the server returns a 4XX status because the request
            is incorrect, not authenticated, or not authorized.
        kafkit.registry.RegistryBrokenError
            Raised if the server returns a 5XX status because something is
            wrong with the server itself.
        """
        if url_vars is None:
            url_vars = {}
        return self._make_request("GET", url, url_vars, b"")

    def get_schema_by_subject(
        self, subject: str, version: str | int = "latest"
    ) -> dict[str, Any]:
        """Get a schema for a subject in the registry.

        Wraps ``GET /subjects/(string: subject)/versions/(versionId: version)``

        Parameters
        ----------
        subject : `str`
            Name of the subject in the Schema Registry.
        version : `int` or `str`, optional
            The version of the schema with respect to the ``subject``. To
            get the latest schema, supply ``"latest"`` (default).

        Returns
        -------
        schema_info : `dict`
            A dictionary with the schema and metadata about the schema. The
            keys are:

            ``"schema"``
                The schema itself, preparsed by `fastavro.parse_schema
                <fastavro._schema_py.parse_schema>`.
            ``"subject"``
                The subject this schema is registered under in the registry.
            ``"version"``
                The version of this schema with respect to the ``subject``.
            ``"id"``
                The ID of this schema (compatible with `get_schema_by_id`).

        See Also
        --------
        get_schema_by_id

        Notes
        -----
        Results from this method are cached locally, so repeated calls are
        fast. Keep in mind that any call with the ``version`` parameter set
        to ``"latest"`` will always miss the cache. The schema is still
        cached, though, under it's true subject version. If you app repeatedly
        calls this method, and you want to make use of caching, replace
        ``"latest"`` versions with integer versions once they're known.
        """
        if isinstance(version, int):
            try:
                # The SubjectCache.get method is designed to have the same
                # return type as this method.
                return self.subject_cache.get(subject, version)
            except ValueError:
                pass

        result = self.get(
            "/subjects{/subject}/versions{/version}",
            url_vars={"subject": subject, "version": str(version)},
        )

        schema = fastavro.parse_schema(json.loads(result["schema"]))

        with contextlib.suppress(TypeError):
            # Can't cache versions like "latest"
            self.subject_cache.insert(
                result["subject"],
                result["version"],
                schema_id=result["id"],
                schema=schema,
            )

        return {
            "id": result["id"],
            "version": result["version"],
            "subject": result["subject"],
            "schema": schema,
        }


def get_day_obs_start_time(day_obs):
    """Get the start of the given dayObs as an astropy.time.Time object.

    The observatory rolls the date over at UTC-12.

    Parameters
    ----------
    day_obs : `int`
        The day_obs, as an integer, e.g. 20231225

    Returns
    -------
    time : `astropy.time.Time`
        The start of the dayObs as an astropy.time.Time object.
    """
    pythonDateTime = datetime.datetime.strptime(str(day_obs), "%Y%m%d")
    return Time(pythonDateTime) + 12 * u.hour


def get_day_obs_end_time(day_obs):
    """Get the end of the given dayObs as an astropy.time.Time object.

    Parameters
    ----------
    day_obs : `int`
        The dayObs, as an integer, e.g. 20231225

    Returns
    -------
    time : `astropy.time.Time`
        The end of the dayObs as an astropy.time.Time object.
    """
    return get_day_obs_start_time(day_obs) + 24 * u.hour


def get_begin_end(
    day_obs=None,
    begin=None,
    end=None,
    timespan=None,
    event=None,
    exp_record=None,
):
    """Calculate the begin and end times to pass to _getEfdData, given the
    kwargs passed to getEfdData.

    Parameters
    ----------
    day_obs : `int | None`
        The dayObs to query. If specified, this is used to determine the begin
        and end times.
    begin : `astropy.time.Time`
        The begin time for the query. If specified, either an end time or a
        timespan must be supplied.
    end : `astropy.time.Time`
        The end time for the query. If specified, a begin time must also be
        supplied.
    timespan : `astropy.time.TimeDelta`
        The timespan for the query. If specified, a begin time must also be
        supplied.
    event : `lsst.summit.utils.efdUtils.TmaEvent`
        The event to query. If specified, this is used to determine the begin
        and end times, and all other options are disallowed.
    exp_record : `lsst.daf.butler.dimensions.DimensionRecord`
        The exposure record containing the timespan to query. If specified, all
        other options are disallowed.

    Returns
    -------
    begin : `astropy.Time`
        The begin time for the query.
    end : `astropy.Time`
        The end time for the query.
    """
    if exp_record is not None:
        forbiddenOpts = [event, begin, end, timespan, day_obs]
        if any(x is not None for x in forbiddenOpts):
            raise ValueError(
                "You can't specify both an expRecord and a "
                "begin/end or timespan or dayObs"
            )
        begin = exp_record.timespan.begin
        end = exp_record.timespan.end
        return begin, end

    if event is not None:
        forbiddenOpts = [begin, end, timespan, day_obs]
        if any(x is not None for x in forbiddenOpts):
            raise ValueError(
                "You can't specify both an event and a "
                "begin/end or timespan or dayObs"
            )
        begin = event.begin
        end = event.end
        return begin, end

    # check for dayObs, and that other options aren't inconsistently specified
    if day_obs is not None:
        forbiddenOpts = [begin, end, timespan]
        if any(x is not None for x in forbiddenOpts):
            raise ValueError(
                "You can't specify both a dayObs and a "
                "begin/end or timespan"
            )
        begin = get_day_obs_start_time(day_obs)
        end = get_day_obs_end_time(day_obs)
        return begin, end
    # can now disregard dayObs entirely

    if begin is None:
        raise ValueError(
            "You must specify either a dayObs or a "
            "begin/end or begin/timespan"
        )
    # can now rely on begin, so just need to deal with end/timespan

    if end is None and timespan is None:
        raise ValueError(
            "If you specify a begin, you must specify either a "
            "end or a timespan"
        )
    if end is not None and timespan is not None:
        raise ValueError("You can't specify both a end and a timespan")
    if end is None:
        if timespan > datetime.timedelta(minutes=0):
            end = begin + timespan  # the normal case
        else:
            end = begin  # the case where timespan is negative
            begin = (
                begin + timespan
            )  # adding the negative to the start, i.e. subtracting it to bring back # noqa: E501

    assert begin is not None
    assert end is not None
    return begin, end


def get_day_obs_for_time(time):
    """Get the dayObs in which an astropy.time.Time object falls.

    Parameters
    ----------
    time : `astropy.time.Time`
        The time.

    Returns
    -------
    dayObs : `int`
        The dayObs, as an integer, e.g. 20231225
    """
    twelveHours = datetime.timedelta(hours=-12)
    offset = TimeDelta(twelveHours, format="datetime")
    return int((time + offset).utc.isot[:10].replace("-", ""))


def efd_timestamp_to_astropy(timestamp):
    """Get an efd timestamp as an astropy.time.Time object.

    Parameters
    ----------
    timestamp : `float`
        The timestamp, as a float.

    Returns
    -------
    time : `astropy.time.Time`
        The timestamp as an astropy.time.Time object.
    """
    return Time(timestamp, format="unix")


def astropy_to_efd_timestamp(time):
    """Get astropy Time object as an efd timestamp

    Parameters
    ----------
    time : `astropy.time.Time`
        The time as an astropy.time.Time object.

    Returns
    -------
    timestamp : `float`
        The timestamp, in UTC, in unix seconds.
    """

    return time.utc.unix


def check_time_format(time_format):
    """Raise exception if time_format not in ['pandas', 'astropy', 'python'].

    Parameters:
    -----------
    time_format: `str`
        The time format to check.

    Raises
    -------
    ValueError: if time_format not in ['pandas', 'astropy', 'python']
    """
    if time_format not in ["pandas", "astropy", "python"]:
        raise ValueError(
            f"format must be one of 'pandas',"
            f"'astropy' or 'python', not {time_format=}"
        )
