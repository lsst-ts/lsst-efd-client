"""Authentication helpers"""

import json
import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

try:
    from lsst.rsp import (
        get_influxdb_credentials,
        list_influxdb_labels,
        UnknownInfluxDBError,
        DiscoveryNotAvailableError,
        TokenNotAvailableError,
    )

    _HAVE_LSST_RSP = True
except ImportError:
    _HAVE_LSST_RSP = False


class NotebookAuth:
    """Class to help keep authentication credentials secret.

    Credentials can be retrieved either from a service endpoint or
    from a file on disk. The credential location is checked in that order.

    Parameters
    ----------
    service_endpoint : `str`, optional
        Endopint of the service to use for credentials.
        (https://roundtable.lsst.codes/segwarides/ by default)
    token : `str`, optional
        If provided, a Gafaelfawr token to use when requesting InfluxDB
        credentials. This need not be provided when running inside a Nublado
        container, where the notebook token will be used, but may be needed
        in other contexts.
    discovery_v1_path : `pathlib.Path`, optional
        If provided, override the path to the Repertoire discovery
        information. This argument is only for testing and should not normally
        be used. The default behavior uses the normal Nublado path for this
        information.

    Raises
    ------
    RuntimeError
        Raised if the service returns a non-200 status code.
    """

    def __init__(
        self,
        service_endpoint="https://roundtable.lsst.codes/segwarides/",
        *,
        token: str | None = None,
        discovery_v1_path: Path | None = None,
    ):
        self.service_endpoint = service_endpoint
        self.token = token
        self._discovery_v1_path = discovery_v1_path

    def get_auth(self, alias):
        """Return the credentials as a tuple

        Parameters
        ----------
        alias : `str`
            Name of the authenticator.

        Returns
        -------
        credentials : `tuple`
            A tuple containing the host name, schema registry, port,
            username, password and path.
        """
        # Try to use local service discovery via lsst.rsp if available. This
        # is the preferred approach inside Nublado notebooks.
        if _HAVE_LSST_RSP:
            try:
                credentials = get_influxdb_credentials(
                    alias,
                    self.token,
                    discovery_v1_path=self._discovery_v1_path
                )
                parsed_url = urlparse(credentials.url)
                return (
                    parsed_url.hostname,
                    credentials.schema_registry,
                    parsed_url.port,
                    credentials.username,
                    credentials.password,
                    parsed_url.path,
                )
            except UnknownInfluxDBError:
                raise ValueError(
                    f"No credentials available for {alias}. "
                    "Try list_auth to get a list of available keys."
                )
            except (DiscoveryNotAvailableError, TokenNotAvailableError):
                pass

        # Second, try the EFDAUTH environment variable. If set, it should
        # point to a JSON file containing a mapping of aliases to InfluxDB
        # connection information as returned from the Repertoire /influxdb
        # endpoint.
        if os.getenv("EFDAUTH"):
            auth_path = Path(os.environ["EFDAUTH"])
            auth_data = json.loads(auth_path.read_text())
            auth_creds = auth_data.get(alias)
            if not auth_creds:
                raise ValueError(
                    f"No credentials available for {alias}. "
                    "Try list_auth to get a list of available keys."
                )
            parsed_url = urlparse(auth_creds["url"])
            return (
                parsed_url.hostname,
                auth_creds["schema_registry"],
                parsed_url.port,
                auth_creds["username"],
                auth_creds["password"],
                parsed_url.path,
            )

        # Fall back on Segwarides. This will stop working in the near future
        # since the Segwarides service is being turned off.
        response = requests.get(
            urljoin(self.service_endpoint, f"creds/{alias}")
        )
        if response.status_code == 200:
            data = response.json()
            return (
                data["host"],
                data["schema_registry"],
                data["port"],
                data["username"],
                data["password"],
                data["path"],
            )
        elif response.status_code == 404:
            raise ValueError(
                f"No credentials available for {alias}. Try list_auth to get a list of available keys."
            )
        else:
            raise RuntimeError(f"Server returned {response.status_code}.")

    def list_auth(self):
        """Return a list of possible credential aliases

        Returns
        -------
        aliases : `list`
            A tuple of `str` that indicate valid aliases to use to retrieve
            credentials.
        """
        if _HAVE_LSST_RSP:
            try:
                return list_influxdb_labels()
            except DiscoveryNotAvailableError:
                pass

        # Second, try the EFDAUTH environment variable. If set, it should
        # point to a JSON file containing a mapping of aliases to InfluxDB
        # connection information as returned from the Repertoire /influxdb
        # endpoint.
        if os.getenv("EFDAUTH"):
            auth_path = Path(os.environ["EFDAUTH"])
            auth_data = json.loads(auth_path.read_text())
            return sorted(auth_data.keys())

        # Fall back on Segwarides. This will stop working in the near future
        # since the Segwarides service is being turned off.
        response = requests.get(urljoin(self.service_endpoint, "list"))
        return response.json()
