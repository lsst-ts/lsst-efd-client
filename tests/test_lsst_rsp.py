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

"""Tests for the authentication helper lsst.rsp integration."""

import pytest

pytest.importorskip("httpx")
pytest.importorskip("pyfakefs")
pytest.importorskip("lsst.rsp")
pytest.importorskip("respx")

import json
from pathlib import Path
from urllib.parse import urlparse

import respx
from httpx import Request, Response
from pyfakefs.fake_filesystem import FakeFilesystem

from lsst_efd_client import NotebookAuth


@pytest.fixture
def discovery_path(fs: FakeFilesystem) -> Path:
    data_path = Path(__file__).parent / "data"
    discovery_path = data_path / "discovery" / "v1.json"
    fs.add_real_directory(data_path)
    fs.add_real_file(discovery_path, target_path="/etc/nublado/discovery/v1.json")
    return discovery_path


def test_lsst_rsp(respx_mock: respx.Router, monkeypatch: pytest.MonkeyPatch, discovery_path: Path) -> None:
    data_path = Path(__file__).parent / "data"
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
        parsed_url.path,
    )

    monkeypatch.setenv("ACCESS_TOKEN", "some-token")

    def handler(request: Request) -> Response:
        assert request.headers["Authorization"] == "Bearer some-token"
        return Response(200, json=data)

    respx_mock.get(credentials_url).mock(side_effect=handler)

    auth = NotebookAuth()
    assert auth.get_auth("idfdev_efd") == expected

    # Try again while passing in an explicit token with no environment
    # variable set.
    monkeypatch.delenv("ACCESS_TOKEN")
    auth = NotebookAuth(token="some-token")
    assert auth.get_auth("idfdev_efd") == expected

    # If lsst.rsp is available, we shouldn't fall back on Segwarides.
    with pytest.raises(ValueError):
        auth.get_auth("test_efd")
