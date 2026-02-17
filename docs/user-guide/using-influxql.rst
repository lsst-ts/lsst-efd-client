.. _using-influxql:

##############
Using InfluxQL
##############

This guide describes how to use the InfluxQL query language with the EFD client.
 
To execute InfluxQL queries you can use the ``_influx_client.query()`` method in conjunction with the EFD client.

.. code::

   from lsst_efd_client import EfdClient

   client = EfdClient("usdf_efd")
   query = '''SELECT vacuum FROM "lsst.sal.ATCamera.vacuum" WHERE time > now() - 1h'''
   await client._influx_client.query(query)

This query returns ``vaccum`` measurements for the ``ATCamera`` in the last hour. 
It uses the the InfluxQL ``now()`` function to query a time range relative to the server's current time.

See `InfluxQL time syntax`_ documentation for detailed information on how to specify time ranges in InfluxQL queries.

The EFD client helper function ``build_time_range_query()`` can also be used to build InfluxQL queries for a topic and time range, which can then be executed with the ``_influx_client.query()`` method.

.. note::
   
   InfluxQL queries are also used in the Chronograf UI, so using InfluxQL queries with the EFD client can be helpful when exploring data in both contexts.

In the following notebooks, you can find more examples using InfluxQL such as downsampling data with ``GROUP by time()``, using aggregation functions, and chunked queries for efficient retrieval of large datasets:

.. grid:: 3

   .. grid-item-card:: Querying the EFD with InfluxQL
      :link: notebooks/UsingInfluxQL.ipynb
      :link-type: url

      Learn how to use the EFD client and InfluxQL to query EFD data.

   .. grid-item-card:: Downsample data with GROUP BY time()
      :link: notebooks/UsingInfluxQL.ipynb
      :link-type: url

      Learn how to downsample data with InfluxQL ``GROUP BY time()``.

   .. grid-item-card:: Chunked queries: Efficient Data Retrieval
      :link: notebooks/ChunkedQueries.ipynb
      :link-type: url

      Learn how to return chunked responses with the EFD client for efficient retrieval of large datasets.

See the `InfluxQL documentation`_ for a complete reference to the InfluxQL capabilities and query syntax.
