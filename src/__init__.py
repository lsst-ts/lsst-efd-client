"""Collection of EFD utilities."""

import importlib.metadata

from lsst_efd_client.auth_helper import NotebookAuth
from lsst_efd_client.efd_helper import EfdClient, EfdClientSync
from lsst_efd_client.efd_utils import (
    merge_packed_time_series,
    rendezvous_dataframes,
    resample,
)

__all__ = [
    "NotebookAuth",
    "EfdClient",
    "EfdClientSync",
    "resample",
    "rendezvous_dataframes",
    "merge_packed_time_series",
]

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
