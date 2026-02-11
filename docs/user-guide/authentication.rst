.. _authentication:

##############
Authentication
##############

This guide describes how EFD client authentication works and how to configure it when running outside the Rubin Science Platform.

Listing available EFD databases
-------------------------------

To list the EFD databases available in your environment:

.. code::

    from lsst.rsp list_influxdb_labels
    
    list_influxdb_labels()

This returns a list of database labels that you can use to connect with the EFD client.
Then you can instantiate the client with:

.. code::

    from lsst_efd_client import EfdClient

    client = EfdClient("<database_label>")

When running the EFD client inside the Rubin Science Platform, the connection information associated with the database label is automatically retrieved from `Repertoire`_ service discovery.

Authentication outside the Rubin Science Platform
--------------------------------------------------

When running the EFD client outside of the Rubin Science Platform, you must configure the connection information manually. 

The connection information must be retrieved from Repertoire and stored in a local file referenced by the ``EFDAUTH`` environment variable.

Inside the Rubin Science Platform environment, you can retrieve the connection information with:

.. code:: Python
    
    from lsst.rsp import get_influxdb_credentials

    get_influxdb_credentials("<database_label>")

Create a local JSON file with the retrieved connection information. 
The JSON file has a mapping of database label to connection information, and allows for multiple entries if needed.

The following example illustrates the required schema of the JSON file using placeholder values:

.. code:: JSON

    {
        "<database_label>": {
            "url": "<database_url>",
            "database": "<database_name>",
            "username": "<username>",
            "password": "<password>",
            "schema_registry": "<schema_registry_url>"
        }
    }

Then set the environment variable ``EFDAUTH`` to the path of the JSON file and instantiate the EFD client with the desired database label.

.. note::

    Do not commit this file to source control, as it contains credentials.

