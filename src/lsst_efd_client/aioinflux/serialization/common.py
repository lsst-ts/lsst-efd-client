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

import warnings

# Special characters documentation:
# https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_reference/#special-characters #noqa
# Although not in the official docs, new line characters are removed in order
# to avoid issues.
# Go implementation:
# https://github.com/influxdata/influxdb/blob/master/pkg/escape/strings.go
key_escape = str.maketrans({"\\": "\\\\", ",": r"\,", " ": r"\ ", "=": r"\=", "\n": ""})
tag_escape = str.maketrans({"\\": "\\\\", ",": r"\,", " ": r"\ ", "=": r"\=", "\n": ""})
str_escape = str.maketrans({"\\": "\\\\", '"': r"\"", "\n": ""})
measurement_escape = str.maketrans({"\\": "\\\\", ",": r"\,", " ": r"\ ", "\n": ""})


def escape(string, escape_pattern):
    """Assistant function for string escaping"""
    try:
        return string.translate(escape_pattern)
    except AttributeError:
        warnings.warn("Non-string-like data passed. Attempting to convert to 'str'.")
        return str(string).translate(tag_escape)
