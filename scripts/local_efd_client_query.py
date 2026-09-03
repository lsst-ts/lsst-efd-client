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

import asyncio
import time

import pandas as pd

from lsst_efd_client import EfdClient

QUERY = " ".join(
    (
        "SELECT mean(temperatureItem0) as mean_temperature FROM",
        '"efd"."autogen"."lsst.sal.ESS.temperature"',
        "where salIndex=301 AND time > now() - 7d GROUP BY time(1m) FILL(linear)",
    )
)

print(f"the query is {QUERY}")


async def test_query_func(client, query=QUERY):
    print(query)
    start = time.time()
    results = await client.influxql_query(query)
    stop = time.time()
    return results, (stop - start)


async def test_chunking(client, query=QUERY):
    # Chunking your queries is much faster....
    start = time.time()
    parts = await client.influxql_query(query, chunked=True, chunk_size=500)
    stop = time.time()
    results = pd.concat([chunk async for chunk in parts])
    return results, (stop - start)


async def test_select_top_n(client):
    start = time.time()
    data = await client.select_top_n(
        "lsst.sal.ESS.temperature",
        "temperatureItem0",
        10,
        index=301,
        convert_influx_index=False,
        use_old_csc_indexing=True,
    )
    stop = time.time()
    return data, stop - start


async def main():
    location = "base_efd"  # "usdf_efd"
    async with EfdClient(location) as client:
        topics = await client.get_topics()
        print(f"topics = {len(topics)}")

        data, elapsed = await test_query_func(client, QUERY)
        print(f"the data we queried for is {len(data)} long")
        print(f"{data} took {elapsed} seconds")

        data, elapsed = await test_chunking(client, QUERY)
        print("\n\n chunky \n\n")
        print(f"{data} took {elapsed} seconds")

        data, elapsed = await test_select_top_n(client)
        print(f"{data} took {elapsed} seconds")


if __name__ == "__main__":
    asyncio.run(main())
