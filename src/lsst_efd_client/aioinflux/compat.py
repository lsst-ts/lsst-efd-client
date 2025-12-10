import warnings

no_pandas_warning = "Pandas/Numpy is not available. \
                    Support for 'dataframe' mode is disabled."

try:
    import numpy as np
    import pandas as pd
except ModuleNotFoundError:
    pd = None
    np = None
    warnings.warn(no_pandas_warning)

__all__ = ["no_pandas_warning", "pd", "np"]
