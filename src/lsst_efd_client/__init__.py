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

"""Collection of EFD utilities."""

import importlib.metadata

from .auth_helper import NotebookAuth
from .efd_helper import EfdClient, EfdClientSync
from .efd_utils import (
    merge_packed_time_series,
    rendezvous_dataframes,
    resample,
)

__all__ = [
    "NotebookAuth",
    "EfdClient",
    "EfdClientSync",
    "resample",
    "rendezvous_dataframes",
    "merge_packed_time_series",
]

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
