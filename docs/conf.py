from documenteer.conf.guide import *  # noqa: F401, F403

# Don’t execute notebooks during the build
nb_execution_mode = "off"

# Don’t treat these as documentation pages (avoids the toctree warnings)
exclude_patterns = [
    "_rst_epilog.rst",
    "**.ipynb",
]
