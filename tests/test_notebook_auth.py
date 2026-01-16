"""Tests for the authentication helper.

The fallback tests using the deprecated Segwarides approach are done in the
main :path:`test_lsst_efd_client.py` tests. The tests for lsst.rsp integration
are done in :path:`test_lsst_rsp.py`.
"""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest

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
        parsed_url.path,
    )
    assert auth.list_auth() == ["idfdev_efd"]

    # If EFDAUTH is set, we shouldn't fall back on Segwarides.
    with pytest.raises(ValueError):
        auth.get_auth("test_efd")
