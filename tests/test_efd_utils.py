# This file is part of summit_utils.
#
# Developed for the LSST Data Management System.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Test cases for utils."""

import contextlib
import datetime
import pathlib
import unittest

import astropy
import pandas as pd
import vcr
from astropy.time import Time

try:
    from lsst.summit.utils.tmaUtils import TMAEvent, TMAState

    lsst_summit_utils_imported = True
except ImportError:
    Warning("Could not import TMAEvent and TMAState")
    lsst_summit_utils_imported = False
from lsst_efd_client import EfdClient, EfdClientSync
from lsst_efd_client.efd_utils import (
    astropy_to_efd_timestamp,
    efd_timestamp_to_astropy,
    get_day_obs_end_time,
    get_day_obs_for_time,
    get_day_obs_start_time,
)

PATH = pathlib.Path(__file__).parent.absolute()

safe_vcr = vcr.VCR(
    record_mode="none",
    cassette_library_dir=str(PATH / "cassettes"),
    path_transformer=vcr.VCR.ensure_suffix(".yaml"),
)


@contextlib.asynccontextmanager
async def make_efd_client():
    efd_client = EfdClient("usdf_efd")
    yield efd_client


@contextlib.contextmanager
def make_synchronous_efd_client():
    efd_client = EfdClientSync("usdf_efd")
    yield efd_client


class EfdUtilsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.day_obs = 20230531
        # get a sample expRecord here to test expRecordToTimespan
        cls.axis_topic = "lsst.sal.MTMount.logevent_azimuthMotionState"
        cls.time_series_topic = "lsst.sal.MTMount.azimuth"
        if lsst_summit_utils_imported:
            cls.event = TMAEvent(
                dayObs=20230531,
                seqNum=27,
                type=TMAState.TRACKING,
                endReason=TMAState.SLEWING,
                duration=0.47125244140625,
                begin=Time(1685578353.2265284, scale="utc", format="unix"),
                end=Time(1685578353.6977808, scale="utc", format="unix"),
                blockInfos=None,
                version=0,
                _startRow=254,
                _endRow=255,
            )

    def test_get_day_obs_as_times(self):
        """This tests getDayObsStartTime and getDayObsEndTime explicitly,
        but the days we loop over are chosen to test calcNextDay() which is
        called by getDayObsEndTime().
        """
        for day_obs in (
            self.day_obs,  # the nominal value
            20200228,  # day before end of Feb on a leap year
            20200229,  # end of Feb on a leap year
            20210227,  # day before end of Feb on a non-leap year
            20200228,  # end of Feb on a non-leap year
            20200430,  # end of a month with 30 days
            20200530,  # end of a month with 31 days
            20201231,  # year rollover
        ):
            dayStart = get_day_obs_start_time(day_obs)
            self.assertIsInstance(dayStart, astropy.time.Time)

            dayEnd = get_day_obs_end_time(day_obs)
            self.assertIsInstance(dayStart, astropy.time.Time)

            self.assertGreater(dayEnd, dayStart)
            self.assertEqual(dayEnd.jd, dayStart.jd + 1)

    @safe_vcr.use_cassette()
    def test_get_topics(self):
        with make_synchronous_efd_client() as efd_client:
            topics = efd_client.get_topics("lsst.sal.MTMount*")
            self.assertIsInstance(topics, list)
            self.assertGreater(len(topics), 0)

            topics = efd_client.get_topics("*fake.topics.does.not.exist*")
            self.assertIsInstance(topics, list)
            self.assertEqual(len(topics), 0)

            # check we can find the mount with a preceding wildcard
            topics = efd_client.get_topics("*mTmoUnt*")
            self.assertIsInstance(topics, list)
            self.assertGreater(len(topics), 0)

            # check it fails if we don't allow case insensitivity
            topics = efd_client.get_topics("*mTmoUnt*", case_sensitive=True)
            self.assertIsInstance(topics, list)
            self.assertEqual(len(topics), 0)

    @safe_vcr.use_cassette()
    def test_get_efd_data(self):
        with make_synchronous_efd_client() as efd_client:
            day_start = get_day_obs_start_time(self.day_obs)
            day_end = get_day_obs_end_time(self.day_obs)
            one_day = datetime.timedelta(hours=24)
            # twelveHours = datetime.timedelta(hours=12)

            # test the dayObs interface
            day_obs_data = efd_client.get_efd_data(
                self.axis_topic, day_obs=self.day_obs
            )
            self.assertIsInstance(day_obs_data, pd.DataFrame)

            # test the starttime interface
            day_start_data = efd_client.get_efd_data(
                self.axis_topic, begin=day_start, timespan=one_day
            )
            self.assertIsInstance(day_start_data, pd.DataFrame)

            # check they're equal
            self.assertTrue(day_obs_data.equals(day_start_data))

            # test the starttime interface with an endtime
            day_end = get_day_obs_end_time(self.day_obs)
            day_start_end_data = efd_client.get_efd_data(
                self.axis_topic, begin=day_start, end=day_end
            )
            self.assertTrue(day_obs_data.equals(day_start_end_data))

            with self.assertRaises(ValueError):
                # not enough info to constrain
                _ = efd_client.get_efd_data(self.axis_topic)
                # dayObs supplied and a start time is not allowed
                _ = efd_client.get_efd_data(
                    self.axis_topic, day_obs=self.day_obs, begin=day_start
                )
                # dayObs supplied and a stop time is not allowed
                _ = efd_client.get_efd_data(
                    self.axis_topic, day_obs=self.day_obs, end=day_end
                )
                # dayObs supplied and timespan is not allowed
                _ = efd_client.get_efd_data(
                    self.axis_topic, day_obs=self.day_obs, timespan=one_day
                )
                # being alone is not allowed
                _ = efd_client.get_efd_data(
                    self.axis_topic, begin=self.day_obs
                )
                # good query, except the topic doesn't exist
                _ = efd_client.get_efd_data(
                    "badTopic", begin=day_start, end=day_end
                )

    @unittest.skipIf(not lsst_summit_utils_imported, "Skipping TMAEvent tests")
    @safe_vcr.use_cassette()
    def test_get_efd_data_event(self):
        with make_synchronous_efd_client() as efd_client:
            # test event
            # note that here we're going to clip to an event and pad things, so
            # we want to use the timeSeriesTopic not the states, so that
            # there's plenty of rows to test the padding is actually working
            day_obs_data = efd_client.get_efd_data(
                self.axis_topic, day_obs=self.day_obs
            )

            event_data = efd_client.get_efd_data(
                self.time_series_topic, event=self.event
            )
            self.assertIsInstance(day_obs_data, pd.DataFrame)

            # test padding options
            padded = efd_client.get_efd_data(
                self.time_series_topic,
                event=self.event,
                pre_padding=1,
                post_padding=2,
            )
            self.assertGreater(len(padded), len(event_data))
            start_time_diff = efd_timestamp_to_astropy(
                event_data.iloc[0]["private_efdStamp"]
            ) - efd_timestamp_to_astropy(padded.iloc[0]["private_efdStamp"])
            end_time_diff = efd_timestamp_to_astropy(
                padded.iloc[-1]["private_efdStamp"]
            ) - efd_timestamp_to_astropy(
                event_data.iloc[-1]["private_efdStamp"]
            )

            self.assertGreater(start_time_diff.sec, 0)
            self.assertLess(
                start_time_diff.sec, 1.1
            )  # padding isn't super exact, so give a little wiggle room  # noqa: E501
            self.assertGreater(end_time_diff.sec, 0)
            self.assertLess(
                end_time_diff.sec, 2.1
            )  # padding isn't super exact, so give a little wiggle room  # noqa: E501

    @safe_vcr.use_cassette()
    def test_get_most_recent_row_with_data_before(self):
        with make_synchronous_efd_client() as efd_client:
            time = Time(1687845854.736784, scale="utc", format="unix")
            rowData = efd_client.get_most_recent_row_with_data_before(
                "lsst.sal.MTM1M3.logevent_forceActuatorState", time
            )
            self.assertIsInstance(rowData, pd.Series)

        stateTime = efd_timestamp_to_astropy(rowData["private_efdStamp"])
        self.assertLess(stateTime, time)

    def test_efd_timestamp_to_astropy(self):
        time = efd_timestamp_to_astropy(1687845854.736784)
        self.assertIsInstance(time, astropy.time.Time)
        return

    def test_astropy_to_efd_timestamp(self):
        time = Time(1687845854.736784, scale="utc", format="unix")
        efd_timestamp = astropy_to_efd_timestamp(time)
        self.assertIsInstance(efd_timestamp, float)
        return

    def test_get_day_obs_fortime(self):
        pydate = datetime.datetime(2023, 2, 5, 13, 30, 1)
        time = Time(pydate)
        day_obs = get_day_obs_for_time(time)
        self.assertEqual(day_obs, 20230205)

        pydate = datetime.datetime(2023, 2, 5, 11, 30, 1)
        time = Time(pydate)
        day_obs = get_day_obs_for_time(time)
        self.assertEqual(day_obs, 20230204)
        return
