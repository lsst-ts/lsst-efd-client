"""Tests for the authentication helper.

The fallback tests using the deprecated Segwarides approach are done in the
main :path:`test_lsst_efd_client.py` tests.
"""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
import respx
from httpx import Request, Response

from lsst_efd_client import NotebookAuth


def test_efdauth(monkeypatch: pytest.MonkeyPatch) -> None:
    efdauth_path = Path(__file__).parent / "data" / "efdauth.json"
    monkeypatch.setenv("EFDAUTH", str(efdauth_path))
    credentials = json.loads(efdauth_path.read_text())
    parsed_url = urlparse(credentials["idfdev_efd"]["url"])

    auth = NotebookAuth()
    assert auth.get_auth("idfdev_efd") == (
        parsed_url.hostname,
        credentials["idfdev_efd"]["schema_registry"],
        parsed_url.port,
        credentials["idfdev_efd"]["username"],
        credentials["idfdev_efd"]["password"],
        parsed_url.path
    )
    assert auth.list_auth() == ["idfdev_efd"]

    # If EFDAUTH is set, we shouldn't fall back on Segwarides.
    with pytest.raises(ValueError):
        auth.get_auth("test_efd")


def test_lsst_rsp(
    respx_mock: respx.Router, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_path = Path(__file__).parent / "data"
    discovery_path = data_path / "discovery" / "v1.json"
    creds_path = data_path / "discovery" / "idfdev_efd.json"
    discovery = json.loads(discovery_path.read_text())
    discovery_data = discovery["influxdb_databases"]["idfdev_efd"]
    credentials_url = discovery_data["credentials_url"]
    data = json.loads(creds_path.read_text())
    parsed_url = urlparse(data["url"])
    expected = (
        parsed_url.hostname,
        data["schema_registry"],
        parsed_url.port,
        data["username"],
        data["password"],
        parsed_url.path
    )

    monkeypatch.setenv("ACCESS_TOKEN", "some-token")

    def handler(request: Request) -> Response:
        assert request.headers["Authorization"] == "Bearer some-token"
        return Response(200, json=data)

    respx_mock.get(credentials_url).mock(side_effect=handler)

    auth = NotebookAuth(discovery_v1_path=discovery_path)
    assert auth.get_auth("idfdev_efd") == expected

    # Try again while passing in an explicit token with no environment
    # variable set.
    monkeypatch.delenv("ACCESS_TOKEN")
    auth = NotebookAuth(token="some-token", discovery_v1_path=discovery_path)
    assert auth.get_auth("idfdev_efd") == expected

    # If lsst.rsp is available, we shouldn't fall back on Segwarides.
    with pytest.raises(ValueError):
        auth.get_auth("test_efd")
