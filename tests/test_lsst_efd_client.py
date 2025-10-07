"""Tests for `lsst_efd_client` package."""

import contextlib
import json
import pathlib

import astropy.units as u
import numpy as np
import pandas as pd
import pytest
import vcr
from aioinflux import InfluxDBClient
from astropy.time import Time, TimeDelta
from kafkit.registry.sansio import MockRegistryApi

from lsst_efd_client import (
    EfdClient,
    EfdClientSync,
    NotebookAuth,
    rendezvous_dataframes,
    resample,
)
from lsst_efd_client.efd_helper import EfdClientTools

PATH = pathlib.Path(__file__).parent.absolute()

# Use mode="none" to run tests for normal operation.
# To update files or generate new ones, make sure you have a working
# connection to lsst-schema-registry-efd.ncsa.illinois.edu
# and temporarily run with mode="once".
safe_vcr = vcr.VCR(
    record_mode="none",
    cassette_library_dir=str(PATH / "cassettes"),
    path_transformer=vcr.VCR.ensure_suffix(".yaml"),
)


@contextlib.asynccontextmanager
async def make_efd_client():
    df = pd.read_hdf(PATH / "efd_test.hdf")
    df1 = pd.read_hdf(PATH / "efd_index_test.hdf", "table")
    async with InfluxDBClient(
        db="client_test",
        mode="async",
        output="dataframe",
    ) as client:
        await client.create_database()
        await client.write(df, measurement="lsst.sal.fooSubSys.test")
        await client.write(df1, measurement="lsst.sal.barSubSys.test")
        efd_client = EfdClient(
            "test_efd", db_name="client_test", client=client
        )
        # Monkey patch the client to point to an existing schema registry
        # Note this is only available if on the NCSA VPN
        efd_client.schema_registry = (
            "https://lsst-schema-registry-efd.ncsa.illinois.edu"
        )
        try:
            yield efd_client
        finally:
            await client.drop_database()


@contextlib.contextmanager
def make_synchronous_efd_client():
    df = pd.read_hdf(PATH / "efd_test.hdf")
    df1 = pd.read_hdf(PATH / "efd_index_test.hdf", "table")
    with InfluxDBClient(
        db="client_test",
        mode="blocking",
        output="dataframe",
    ) as client:
        client.create_database()
        client.write(df, measurement="lsst.sal.fooSubSys.test")
        client.write(df1, measurement="lsst.sal.barSubSys.test")
        efd_client = EfdClientSync(
            "test_efd", db_name="client_test", client=client
        )
        # Monkey patch the client to point to an existing schema registry
        # Note this is only available if on the NCSA VPN
        efd_client.schema_registry = (
            "https://lsst-schema-registry-efd.ncsa.illinois.edu"
        )
        try:
            yield efd_client
        finally:
            client.drop_database()


def get_expected_strs():
    expected = []
    with open(PATH / "expected.txt", "r") as fh:
        for line in fh.readlines():
            expected.append(line)
    return expected


@pytest.fixture
def test_df():
    return pd.read_hdf(PATH / "efd_test.hdf")


@pytest.fixture
def test_query_res():
    return pd.read_hdf(PATH / "packed_data.hdf", key="test_data")


@pytest.fixture
def start_stop():
    time = Time("2020-01-28T23:07:19.00", format="isot", scale="utc")
    return (time, time + TimeDelta(600, format="sec"))


@pytest.fixture
def start_stop_old():
    time = Time("2020-01-27T23:07:19.00", format="isot", scale="utc")
    return (time, time + TimeDelta(600, format="sec"))


def test_bad_endpoint():
    with pytest.raises(IOError):
        NotebookAuth(service_endpoint="https://no.path.here.net.gov")


@safe_vcr.use_cassette()
def test_get_auth():
    auth_creds = NotebookAuth().get_auth("test_efd")
    assert auth_creds[0] == "foo.bar.baz.net"
    assert auth_creds[1] == "https://schema-registry-foo.bar.baz"
    assert auth_creds[2] == "443"
    assert auth_creds[3] == "foo"
    assert auth_creds[4] == "bar"


@safe_vcr.use_cassette()
def test_auth_list():
    auth_client = NotebookAuth()
    # Make sure there is at least one set of credentials
    # other than the test one used here
    assert len(auth_client.list_auth()) > 1


@safe_vcr.use_cassette()
def test_efd_names():
    # Don't assume same order in case we change
    # the backend to something that doesn't
    # guarantee that
    auth_client = NotebookAuth()
    assert set(list(EfdClient.list_efd_names())) == set(
        auth_client.list_auth()
    )


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_build_query(start_stop):
    expected_strs = get_expected_strs()
    # Check passing a single field works
    async with make_efd_client() as efd_client:
        qstr = efd_client.build_time_range_query(
            "lsst.sal.fooSubSys.test", "foo", start_stop[0], start_stop[1]
        )
        assert qstr == expected_strs[0].strip()
        # Check passing a list of fields works
        qstr = efd_client.build_time_range_query(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            start_stop[0],
            start_stop[1],
        )
        assert qstr == expected_strs[1].strip()
        # Check old indexed component fetching works
        qstr = efd_client.build_time_range_query(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            start_stop[0],
            start_stop[1],
            index=2,
            use_old_csc_indexing=True,
        )
        assert qstr == expected_strs[4].strip()
        # Check new indexed component fetching works
        qstr = efd_client.build_time_range_query(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            start_stop[0],
            start_stop[1],
            index=2,
        )
        assert qstr == expected_strs[5].strip()


@pytest.mark.asyncio
async def test_build_query_delta(start_stop):
    # Run `test_build_query` with the same times,
    # but expressed as (start_time, delta)
    tdelta = start_stop[1] - start_stop[0]
    await test_build_query(start_stop=(start_stop[0], tdelta))


@pytest.mark.asyncio
async def test_build_query_tai(start_stop):
    # Run `test_build_query` with the same times, but expressed as TAI.
    await test_build_query(start_stop=[val.tai for val in start_stop])


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_parse_schema():
    """Test the EfdClient._parse_schema method."""
    # Body that we expect the registry API to return given the request.
    expected_body = {
        "schema": json.dumps(
            {
                "name": "schema1",
                "type": "record",
                "fields": [
                    {
                        "name": "a",
                        "type": "int",
                        "description": "Description 1",
                        "units": "second",
                    },
                    {
                        "name": "b",
                        "type": "double",
                        "description": "Description 2",
                        "units": "meter",
                    },
                    {
                        "name": "c",
                        "type": "float",
                        "description": "Description 3",
                        "units": "gram",
                    },
                    {
                        "name": "d",
                        "type": "string",
                        "description": "Description 4",
                        "units": "torr",
                    },
                ],
            }
        ),
        "subject": "schema1",
        "version": 1,
        "id": 2,
    }

    body = json.dumps(expected_body).encode("utf-8")
    client = MockRegistryApi(body=body)

    schema = await client.get_schema_by_subject("schema1")
    result = EfdClientTools.parse_schema("schema1", schema)
    assert isinstance(result, pd.DataFrame)
    for i, name in enumerate("abcd"):
        assert result["name"][i] == name
    for i in range(4):
        assert result["description"][i] == f"Description {i+1}"
    assert "units" in result.columns
    assert "aunits" in result.columns
    assert "type" not in result.columns
    for unit in result["aunits"]:
        assert isinstance(unit, u.UnitBase)


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_bad_units():
    """Test that the EfdClient._parse_schema method raises when a bad astropy
    unit definition is passed.
    """
    # Body that we expect the registry API to return given the request.
    expected_body = {
        "schema": json.dumps(
            {
                "name": "schema1",
                "type": "record",
                "fields": [
                    {
                        "name": "a",
                        "type": "int",
                        "description": "Description 1",
                        "units": "not_unit",
                    },
                ],
            }
        ),
        "subject": "schema1",
        "version": 1,
        "id": 2,
    }

    body = json.dumps(expected_body).encode("utf-8")
    client = MockRegistryApi(body=body)

    schema = await client.get_schema_by_subject("schema1")
    with pytest.raises(ValueError):
        EfdClientTools.parse_schema("schema1", schema)


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_topics():
    async with make_efd_client() as efd_client:
        topics = await efd_client.get_topics()
        assert len(topics) == 2
        assert topics[1] == "lsst.sal.fooSubSys.test"
        assert topics[0] == "lsst.sal.barSubSys.test"


@safe_vcr.use_cassette("test_topics.yaml")
def test_topics_sync():
    with make_synchronous_efd_client() as efd_client:
        topics = efd_client.get_topics()
        assert len(topics) == 2
        assert topics[1] == "lsst.sal.fooSubSys.test"
        assert topics[0] == "lsst.sal.barSubSys.test"


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_fields(test_df):
    async with make_efd_client() as efd_client:
        columns = await efd_client.get_fields("lsst.sal.fooSubSys.test")
        for c in test_df.columns:
            assert c in columns


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_time_series(start_stop, start_stop_old):
    async with make_efd_client() as efd_client:
        df = await efd_client.select_time_series(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            start_stop[0],
            start_stop[1],
        )
        assert len(df) == 600
        for c in ["foo", "bar"]:
            assert c in df.columns
        df_legacy = await efd_client.select_time_series(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            start_stop[0],
            start_stop[1],
            convert_influx_index=True,
        )
        # Test that df_legacy is in UTC assuming df was in TAI
        t = Time(df.index).unix - Time(df_legacy.index).unix
        # The indexes should all be the same since both the time range
        # and index were shifted
        assert np.all(t == 0.0)
        # But the queries should be different
        assert not efd_client.query_history[-2] == efd_client.query_history[-1]
        # Test indexed query
        df1 = await efd_client.select_time_series(
            "lsst.sal.barSubSys.test",
            ["eggs", "ham"],
            start_stop[0],
            start_stop[1],
            index=2,
        )
        assert len(df1) == 100
        assert np.all(df1["eggs"] == 10)
        # Old index should return similar sized dataframe
        df1 = await efd_client.select_time_series(
            "lsst.sal.barSubSys.test",
            ["eggs", "ham"],
            start_stop_old[0],
            start_stop_old[1],
            index=2,
            use_old_csc_indexing=True,
        )
        assert len(df1) == 100
        assert np.all(df1["eggs"] == 10)
        # Using new indexing across old time frame should return an empty
        # dataframe
        df1 = await efd_client.select_time_series(
            "lsst.sal.barSubSys.test",
            ["eggs", "ham"],
            start_stop_old[0],
            start_stop_old[1],
            index=2,
        )
        assert len(df1) == 0
        # Using old indexing across new time frame should return an empty
        # dataframe
        df1 = await efd_client.select_time_series(
            "lsst.sal.barSubSys.test",
            ["eggs", "ham"],
            start_stop[0],
            start_stop[1],
            index=2,
            use_old_csc_indexing=True,
        )
        assert len(df1) == 0


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_top_n(start_stop):
    async with make_efd_client() as efd_client:
        df = await efd_client.select_top_n(
            "lsst.sal.fooSubSys.test", ["foo", "bar"], 10
        )
        assert len(df) == 10
        for c in ["foo", "bar"]:
            assert c in df.columns
        df_legacy = await efd_client.select_top_n(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            10,
            convert_influx_index=True,
        )
        # Test that df_legacy is in UTC assuming df was in TAI
        t = Time(df.index).unix - Time(df_legacy.index).unix
        assert np.all(t == 37.0)
        df = await efd_client.select_top_n(
            "lsst.sal.fooSubSys.test",
            ["foo", "bar"],
            10,
            time_cut=start_stop[0],
        )
        assert len(df) == 10
        for c in ["foo", "bar"]:
            assert c in df.columns
        assert df["foo"].values[0] == 144.11835565266966
        assert df["bar"].values[0] == 631.1982694645203
        assert df["foo"].values[-1] == 180.95267940509046
        assert df["bar"].values[-1] == 314.7001662962593


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_packed_time_series(start_stop, test_query_res):
    async with make_efd_client() as efd_client:
        df_exp = test_query_res
        df = await efd_client.select_packed_time_series(
            "lsst.sal.fooSubSys.test",
            ["ham", "egg", "hamegg"],
            start_stop[0],
            start_stop[1],
        )
        # The column 'times' holds the input to the packed time index.
        # It's typically in TAI, but the returned index should be in UTC
        assert np.all((np.array(df["times"]) - Time(df.index).unix) == 37.0)
        assert np.all((df.index[1:] - df.index[:-1]).total_seconds() > 0)
        assert np.all(df == df_exp)
        for c in ["ham", "egg"]:
            assert c in df.columns


@pytest.mark.asyncio
@safe_vcr.use_cassette()
async def test_non_existing_topic(start_stop):
    async with make_efd_client() as efd_client:
        with pytest.raises(ValueError):
            await efd_client.select_time_series(
                "non.existing.topic",
                ["ham", "egg"],
                start_stop[0],
                start_stop[1],
            )


def test_resample(test_query_res):
    df = test_query_res
    df_copy = df.copy()
    df_copy.set_index(
        df_copy.index + pd.Timedelta(0.05, unit="s"), inplace=True
    )
    df_out = resample(df, df_copy)
    assert len(df_out) == 2 * len(df)


def test_rendezvous(test_df):
    sub = test_df.iloc[[25, 75], :]
    # this makes sure the index is not the same, which is the
    # point of this helper method
    sub.set_index(sub.index + pd.Timedelta(0.5, unit="s"), inplace=True)
    merged = rendezvous_dataframes(test_df, sub)
    for i, rec in enumerate(merged.iterrows()):
        if i < 26:
            assert np.isnan(rec[1]["ham0_y"])
            assert np.isnan(rec[1]["egg0_y"])
        elif i > 25 and i < 76:
            assert rec[1]["ham0_y"] == sub["ham0"][0]
            assert rec[1]["egg0_y"] == sub["egg0"][0]
        elif i > 75:
            assert rec[1]["ham0_y"] == sub["ham0"][1]
            assert rec[1]["egg0_y"] == sub["egg0"][1]
