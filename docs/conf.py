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

from documenteer.conf.guide import *  # noqa: F401, F403

# Don’t execute notebooks during the build
nb_execution_mode = "off"

# Don’t treat these as documentation pages (avoids the toctree warnings)
exclude_patterns = [
    "_rst_epilog.rst",
    "**.ipynb",
]

linkcheck_ignore = [
    r"https://usdf-rsp.slac.stanford.edu/repertoire/discovery/influxdb",
]
