"""Tests for the authentication helper.

The fallback tests using the deprecated Segwarides approach are done in the
main :path:`test_lsst_efd_client.py` tests.
"""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
import vcr

from lsst_efd_client import NotebookAuth

PATH = Path(__file__).parent.absolute()

# VCR configuration for recording/replaying HTTP requests
safe_vcr = vcr.VCR(
    record_mode="none",
    cassette_library_dir=str(PATH / "cassettes"),
    path_transformer=vcr.VCR.ensure_suffix(".yaml"),
)


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
        parsed_url.path,
    )
    assert auth.list_auth() == ["idfdev_efd"]

    # If EFDAUTH is set, we shouldn't fall back on Segwarides.
    with pytest.raises(ValueError):
        auth.get_auth("test_efd")


@safe_vcr.use_cassette()
def test_lsst_rsp(monkeypatch: pytest.MonkeyPatch) -> None:
    data_path = Path(__file__).parent / "data"
    discovery_path = data_path / "discovery" / "v1.json"
    creds_path = data_path / "discovery" / "idfdev_efd.json"
    data = json.loads(creds_path.read_text())
    parsed_url = urlparse(data["url"])
    expected = (
        parsed_url.hostname,
        data["schema_registry"],
        parsed_url.port,
        data["username"],
        data["password"],
        parsed_url.path,
    )

    monkeypatch.setenv("ACCESS_TOKEN", "some-token")

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
