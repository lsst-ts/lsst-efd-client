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

# flake8: noqa 402
from ..compat import pd

if pd:
    from . import dataframe
from . import mapping


def serialize(data, measurement=None, tag_columns=None, **extra_tags):
    """Converts input data into line protocol format"""
    if isinstance(data, bytes):
        return data
    elif isinstance(data, str):
        return data.encode("utf-8")
    elif hasattr(data, "to_lineprotocol"):
        return data.to_lineprotocol()
    elif pd is not None and isinstance(data, pd.DataFrame):
        return dataframe.serialize(
            data, measurement, tag_columns, **extra_tags
        )
    elif isinstance(data, dict):
        return mapping.serialize(data, measurement, **extra_tags)
    elif hasattr(data, "__iter__"):
        return b"\n".join(
            [
                serialize(i, measurement, tag_columns, **extra_tags)
                for i in data
            ]
        )
    else:
        raise ValueError("Invalid input", data)
