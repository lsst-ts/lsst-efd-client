# Change log

## 0.14.0 (2025-12-12)

### New features

- Unpin numpy in pyproject.toml
- Add aioinflux source as a module to lsst_efd_client and remove it as an external import.

### Other changes

- Migrate packaging to use `pyproject.toml`.
- Update documentation to use the Rubin user guide theme from Documenteer.
- Add GitHub Actions for continuous integration.
- Add tox for test orchestration.

## 0.13.2 (2025-10-06)

### New features

- Adding conda packaging according to telescope and site standards.

## 0.13.1 (2023-07-01)

### Bug fixes

- Pin numpy to version 1.23.5. numpy 1.25.0 breaks compatibility with aioinflux.

## 0.13.0 (2023-06-26)

### New features

- `EfdClient` now accepts non-UTC `astropy.time.Time` values (converting them, if necessary).

### Bug fixes

- Fix unit tests:

  - Update `casettes/*.yaml` to include "path" as a returned value.
  - Change fixture `efd_client` to an async context manager `make_efd_client` and call it explicitly.
  - Import `vcr` explicitly and configure it so that it fails if the specified cassette files cannot be found or used.
  - Consolidate a few tests to make it clearer that they are testing the same thing, and to reduce the number of cassette files needed.

### Other changes

- Reformat the codebase with black and isort.

## 0.12.0 (2022-07-27)

### New features

- The auth helper can now use a path attribute to connect to an InfluxDB instance.

## 0.11.0 (2022-05-24)

- Updates to API calls to handle SAL 7 change to indexed component ID field.
- Add timeout option to the EFD client.
  Set the `aiohttp.ClientSession` timeout when working with chunked queries. Set a default timeout of 900 seconds.

## 0.10.2 (2022-05-03)

### Bug fixes

- Allow astropy to raise an exception for malformed units in the topic schema.

## 0.10.0 (2021-11-18)

### New features

- Change how ports are handled.
  This forces the port to be sent with the rest of the auth information and removes the ability to pass an override port.

## 0.9.1 (2021-10-28)

### Bug fixes

- Fix various bugs left over from the UTC conversions.
  The most important of these is correcting how the index for packed time series is handled.
  The other is in the implementation fow how we attempt to handle legacy databases with the index still in TAI.

## 0.9.0 (2021-10-05)

### New features

- This changes the convention to using UTC as the internal representation.
  This mirrors a change in InfluxDB to store index timestamps as UTC.
  There is a switch to convert the index from TAI to UTC, but the default is to assume UTC everywhere.

## 0.8.3 (2021-06-11)

### Bug fixes

- Add column specifying if a field in the schema is array-like.
- Check for both conventions indicating unitless columns when creating astropy units.

## 0.8.2 (2021-05-25)

### Bug fixes

- Deal gracefully with topics that have no description or units (Thanks Angelo!)
- Add a parameter to allow a time threshold when selecting top N (Thanks Michael!)

## 0.7.0 (2021-03-24)

### New features

- Add the get_schema method to fetch metadata about topic fields.
- Add a test for the schema parser.

## 0.6.4 (2021-02-15)

### Bug fixes

- Fix a regression that causes an exception when the name of one column is the same as the start of the name of another column.
- Add a test for the regression.

## 0.6.0 (2021-02-11)

### New features

- Add some utility functionality and speed up unpacking packed time series.

### Bug fixes

- Fix bug in listing possible endpoints

## 0.5.0 (2020-05-29)

### New features

- Switch to the segwarides based credentials.

## 0.4.0 (2019-01-31)

### Other changes

- Documentation updates.

## 0.3.0 (2019-01-31)

### Other changes

- Unit tests added.

## 0.2.0 (2019-01-15)

### New features

- Minimal functionality in place.
- Docstrings complete.

## 0.1.0 (2019-12-23)

### New features

- First release on PyPI.
