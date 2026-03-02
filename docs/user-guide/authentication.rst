.. _authentication:

##############
Authentication
##############

This guide describes how authentication for the EFD client works both inside and outside the Rubin Science Platform (RSP).

.. note::

    The EFD client is migrating to use the RSP Repertoire service discovery.
    The current authentication method with Segwarides will be deprecated in the future. 
    
    When running the EFD client outside the RSP, you should use the new Repertoire-based authentication method described below.
    
Authentication inside the RSP
=============================

When running the EFD client inside the RSP, authentication is handled automatically by `Repertoire`_ service discovery. 

To list the available EFD databases in your RSP environment:

.. code::

    from lsst.rsp import list_influxdb_labels
    
    list_influxdb_labels()

This returns a list of database labels that can be used to initialize the EFD client:

.. code::

    from lsst_efd_client import EfdClient

    client = EfdClient("<database_label>")


Authentication outside the RSP
==============================

When running the EFD client outside the RSP, you must:

1.	Retrieve the connection information from Repertoire service discovery API.
2.	Store the connection information in a local JSON file.
3.	Set the ``EFDAUTH`` environment variable to point to that file.

The Repertoire service discovery API provides the ``/repertoire/discovery/influxdb`` endpoint to retrieve connection information for available EFD databases.
This endpoint is accessible from within the RSP and requires authentication.

.. button-link:: https://usdf-rsp.slac.stanford.edu/repertoire/discovery/influxdb
   :color: primary
   :outline:
   :expand:

   Retrieve US Data Facility EFD connection information

The JSON file must contain a mapping of the database labels to the connection information required by the EFD client.

.. note::

    Do not commit this file to source control, as it contains credentials.
