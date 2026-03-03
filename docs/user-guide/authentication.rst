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

When running the EFD client outside the RSP, you have to provide the database connection information in a local JSON file following these steps:

1.	Retrieve the database connection information from Repertoire

The Repertoire service discovery API provides the ``/repertoire/discovery/influxdb`` endpoint to retrieve connection information to EFD databases.
You must be authenticated to the RSP in your browser to access this endpoint.

For example, to retrieve the connection information for the USDF EFD, open the following URL in your browser:

``https://usdf-rsp.slac.stanford.edu/repertoire/discovery/influxdb``

The JSON response contains a mapping of the database label to the connection information required by the EFD client.

2.	Copy the the JSON response and store in a local file, e.g. ``~/.efdauth.json``.

3.	Set the ``EFDAUTH`` environment variable to point to this file before initializing the EFD client:

.. code::

    import os
    from lsst_efd_client import EfdClient

    os.environ["EFDAUTH"] =  os.path.expanduser("~/.efdauth.json")
    client = EfdClient("<database_label>")
