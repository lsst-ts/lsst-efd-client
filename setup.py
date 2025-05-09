import setuptools
import setuptools_scm

setuptools.setup(
    version=setuptools_scm.get_version(
        write_to="src/lsst_efd_client/version.py"
    )
)
