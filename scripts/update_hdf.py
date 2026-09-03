# This file is part of lsst-efd-client.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""
Patch HDF5 files written by older pandas that store datetime64 with no unit.

This script updates node attrs where value_type == "datetime64" to
"datetime64[ns]" so recent pandas can read the file.
"""

import pathlib
import sys

import tables


def iter_hdf_files(paths: list[pathlib.Path]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.glob("*.hdf")))
        else:
            files.append(path)
    return files


def patch_file(path: pathlib.Path) -> int:
    updated = 0
    with tables.open_file(path, mode="r+") as h5:
        for node in h5.walk_nodes("/"):
            attrs = getattr(node, "_v_attrs", None)
            if attrs is None:
                continue
            if "value_type" in attrs:
                if attrs["value_type"] == "datetime64":
                    attrs["value_type"] = "datetime64[ns]"
                    updated += 1
    return updated


def main() -> int:
    files = iter_hdf_files([pathlib.Path("tests")])
    if not files:
        print("No .hdf files found.", file=sys.stderr)
        return 2

    total = 0
    for path in files:
        total += patch_file(path)

    print(f"Done. Updated {total} node(s).")
    return 0


if __name__ == "__main__":
    main()
