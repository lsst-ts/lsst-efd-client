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

import inspect
from typing import Callable, Generator, Optional


def iterpoints(resp: dict, parser: Optional[Callable] = None) -> Generator:
    """Iterates a response JSON yielding data point by point.

    Can be used with both regular and chunked responses.
    By default, returns just a plain list of values representing each point,
    without column names, or other metadata.

    In case a specific format is needed, an optional ``parser`` argument can
    be passed.
    ``parser`` is a function/callable that takes data point values
    and, optionally, a ``meta`` parameter containing which takes a
    dictionary containing all or a subset of the following:
    ``{'columns', 'name', 'tags', 'statement_id'}``.

    Sample parser functions:

    .. code:: python

       # Function optional meta argument
       def parser(*x, meta):
           return dict(zip(meta['columns'], x))

       # Namedtuple (callable)
       from collections import namedtuple
       parser = namedtuple('MyPoint', ['col1', 'col2', 'col3'])


    :param resp: Dictionary containing parsed JSON
        (output from InfluxDBClient.query)
    :param parser: Optional parser function/callable
    :return: Generator object
    """
    for statement in resp["results"]:
        if "series" not in statement:
            continue
        for series in statement["series"]:
            if parser is None:
                yield from (x for x in series["values"])
            elif "meta" in inspect.signature(parser).parameters:
                meta = {k: series[k] for k in series if k != "values"}
                meta["statement_id"] = statement["statement_id"]
                yield from (parser(*x, meta=meta) for x in series["values"])
            else:
                yield from (parser(*x) for x in series["values"])
