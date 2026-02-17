.. _working-with-timestamps:

#######################
Working with timestamps
#######################

This guide describes which timestamps are available in telemetry topics and how to use them when querying the EFD.

The Observatory Control System (OCS) generally stores the time at which telemetry is measured in the ``timestamp`` field. 
When present, this is the preferred timestamp for analysis.

To determine whether a topic includes this field, check the topic schema.

For example, for the ``MTMount.azimuth`` telemetry topic:

.. code:: Python

   from lsst_efd_client import EfdClient
   client = EfdClient("usdf_efd")

   await client.get_schema("lsst.sal.MTMount.azimuth")
   
This returns the fields available in the topic schema, including the ``timestamp`` field:

.. code::

   name        description                                               units      aunits   is_array

   timestamp   Time at which the data was measured (TAI, unix seconds).  second     s        False


Telemetry topics may include multiple timestamp fields:

- ``timestamp`` - Time at which the data was measured (TAI, unix seconds).
- ``private_sndStamp`` - Time at which the message was published by the OCS (TAI, unix seconds). Use this when ``timestamp`` is not available, as it should be very close to the time at which the telemetry was measured.
- ``private_efdStamp`` - UTC timestamp derived from ``private_sndStamp``.
- ``time`` - Database time index, same as ``private_efdStamp``.


TAI vs. UTC
===========

OCS timestamps are in TAI, but the EFD ``time`` field is in UTC. 
Always convert timestamps to UTC when querying the EFD, you can use the `astropy.time`_ package to perform this conversion:

.. code:: Python

	from astropy.time import Time

	start_utc = Time(start_tai, scale="tai").utc
	end_utc = Time(end_tai, scale="tai").utc

Then use the UTC timestamps in your EFD query:

.. code:: Python
	
    await client.select_time_series("lsst.sal.MTMount.azimuth",  ["actualPosition"], start_utc, end_utc)

