# This file is part of lsst-efd-client.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

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
