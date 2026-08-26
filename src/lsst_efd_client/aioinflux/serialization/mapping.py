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

import time
from typing import Mapping

import ciso8601

from .common import (
    escape,
    key_escape,
    measurement_escape,
    str_escape,
    tag_escape,
)


def serialize(point: Mapping, measurement=None, **extra_tags) -> bytes:
    """Converts dictionary-like data into single line protocol line (point)"""
    tags = _serialize_tags(point, extra_tags)
    return (
        f"{_serialize_measurement(point, measurement)}"
        f"{',' if tags else ''}{tags} "
        f"{_serialize_fields(point)} "
        f"{_serialize_timestamp(point)}"
    ).encode()


def _serialize_measurement(point, measurement):
    try:
        return escape(point["measurement"], measurement_escape)
    except KeyError:
        if measurement is None:
            raise ValueError("'measurement' missing")
        return escape(measurement, measurement_escape)


def _serialize_tags(point, extra_tags):
    output = []
    for k, v in {**point.get("tags", {}), **extra_tags}.items():
        k = escape(k, key_escape)
        v = escape(v, tag_escape)
        if not v:
            continue  # ignore blank/null string tags
        output.append(f"{k}={v}")
    return ",".join(output)


def _serialize_timestamp(point):
    dt = point.get("time")
    if not dt:
        return ""
    elif isinstance(dt, int):
        return dt
    elif isinstance(dt, (str, bytes)):
        dt = ciso8601.parse_datetime(dt)
        if not dt:
            raise ValueError(f"Invalid datetime string: {dt!r}")

    if not dt.tzinfo:
        # Assume tz-naive input to be in UTC, not local time
        return int(dt.timestamp() - time.timezone) * 10**9 + dt.microsecond * 1000
    return int(dt.timestamp()) * 10**9 + dt.microsecond * 1000


def _serialize_fields(point):
    """Field values can be floats, integers, strings, or Booleans."""
    output = []
    for k, v in point["fields"].items():
        k = escape(k, key_escape)
        if isinstance(v, bool):
            output.append(f"{k}={v}")
        elif isinstance(v, int):
            output.append(f"{k}={v}i")
        elif isinstance(v, str):
            output.append(f'{k}="{v.translate(str_escape)}"')
        elif v is None:
            # Empty values
            continue
        else:
            # Floats
            output.append(f"{k}={v}")
    return ",".join(output)
