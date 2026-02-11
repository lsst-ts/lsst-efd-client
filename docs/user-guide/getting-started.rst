###############
Getting started
###############

Installation
============

The LSST EFD Client is preinstalled in the ``rubin-env-rsp`` and ``rubin-env-developer`` Conda-Forge metapackages, which is the default Python environment for the `Rubin Science Platform`_.
You can check if ``lsst_efd_client`` is available at a Python prompt:

.. prompt:: python >>>

   import lsst_efd_client

If not available, you can install with either Conda or pip:

.. tab-set::

   .. tab-item:: pip

      .. prompt:: bash

         pip install lsst-efd-client

   .. tab-item:: Conda

      .. prompt:: bash

         conda install -c conda-forge lsst-efd-client


Quick start
===========

Instantiate the EFD client from the Rubin Science Platform by specifying the label of the database instance you want to connect to.

For example, connect to the US Data Facility EFD use the ``usdf_efd`` label:

.. code::

   from lsst_efd_client import EfdClient

   client = EfdClient("usdf_efd")
   await client.get_topics()

This call returns the list of available telemetry topics, confirming that the client is connected successfully.


List available databases
------------------------

To see which databases are available on your current Science Platform environment, run:

.. code::

   from lsst_efd_client import EfdClient
   EfdClient.list_auth()

This will return a list of database labels that you can use to connect with the EFD client.

If you are running the EFD client outside of the Rubin Science Platform, see the :ref:`authentication` section for instructions on how to set up access to the EFD databases.
