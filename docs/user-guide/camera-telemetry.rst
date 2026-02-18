.. _camera-telemetry:

################
Camera telemetry
################

This guide describes how to use the EFD client to query camera telemetry.

Currently the Camera Control System (CCS) stores camera telemetry into three separate databases ``lsst.ATCamera``, ``lsst.CCCamera`` and ``lsst.MTCamera`` for LATISS, ComCam and LSSTCam instruments respectively.

Eventually this data will be merged into the same database used by the Observatory Control System (OCS), but for now you have to initialize the EFD client with the appropriate database name to access camera telemetry:

.. code:: Python

    from lsst_efd_client import EfdClient 

    client = EfdClient("usdf_efd", db_name="lsst.MTCamera")

The camera telemetry databases have a similar structure to the OCS EFD database, but with some important differences in the schema.
The camera telemetry topics have the ``timestamp`` field in UTC which corresponds to the CCS timestamps, and do not include the additional timestamps added by the OCS such as ``private_sndStamp`` or ``private_efdStamp``.

The camera telemetry topics also include tags with metadata which can be used to filter and ``GROUP BY`` data in InfluxQL queries.

For example, the ``lsst.MTCamera.focal_plane_aspic_temp`` topic has temperatures from the focal plane ASPICs.
Check the topic schema for a description of the available fields and tags:

.. code:: Python

    await client.get_schema("lsst.MTCamera.focal_plane_aspic_temp")

In particular, tags can be listed with the ``get_tags()`` method:

.. code:: Python

    await client.get_tags("lsst.MTCamera.focal_plane_aspic_temp")

You can use these tags to filter and group data in your queries, for example to get the average ASPIC temperatures in the last minute for a particular Raft:

.. code:: Python

    query = '''SELECT mean(temperature) FROM "lsst.MTCamera.focal_plane_aspic_temp" WHERE time > now() - 1m AND Raft='R01' GROUP BY Raft'''
    await client._influx_client.query(query)
    
Note that tag values are single quoted in InfluxQL queries.



