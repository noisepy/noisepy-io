import os
from datetime import datetime, timezone

import pytest
from datetimerange import DateTimeRange
from test_channelcatalog import MockCatalog

from noisepy.seis.io.s3store import NCEDCS3DataStore

timespan1 = DateTimeRange(
    datetime(2022, 1, 2, tzinfo=timezone.utc), datetime(2022, 1, 3, tzinfo=timezone.utc)
)
timespan2 = DateTimeRange(
    datetime(2021, 2, 3, tzinfo=timezone.utc), datetime(2021, 2, 4, tzinfo=timezone.utc)
)
timespan3 = DateTimeRange(
    datetime(2023, 6, 1, tzinfo=timezone.utc), datetime(2023, 6, 2, tzinfo=timezone.utc)
)
files_dates = [
    ("AFD.NC.HHZ..D.2022.002", timespan1),
    ("KCPB.NC.HHN..D.2021.034", timespan2),
    ("LMC.NC.HHN..D.2023.152", timespan3),
]


@pytest.mark.parametrize("file,expected", files_dates)
def test_parsefilename2(file: str, expected: DateTimeRange):
    assert expected == NCEDCS3DataStore._parse_timespan(None, file)


data_paths = [
    (os.path.join(os.path.dirname(__file__), "./data/ncedc/2022/2022.002/"), None),
    ("s3://ncedc-pds/continuous_waveforms/BK/2022/2022.002/", None),
    ("s3://ncedc-pds/continuous_waveforms/BK/", timespan1),
]


read_channels = [
    (NCEDCS3DataStore._parse_channel(None, "YUBA.BK.LHZ.00.D.2022.002")),
    (NCEDCS3DataStore._parse_channel(None, "RUSS.BK.LHZ.00.D.2022.002")),
    (NCEDCS3DataStore._parse_channel(None, "THIS.BK.LHZ.00.D.2022.002")),
]


@pytest.fixture(params=data_paths)
def store(request):
    storage_options = {"s3": {"anon": True}}
    (path, timespan) = request.param
    return NCEDCS3DataStore(path, MockCatalog(), lambda ch: ch in read_channels, timespan, storage_options)


@pytest.mark.parametrize("channel", read_channels)
def test_read(store: NCEDCS3DataStore, channel: str):
    chdata = store.read_data(timespan1, channel)
    assert chdata.sampling_rate == 1.0
    assert chdata.start_timestamp >= timespan1.start_datetime.timestamp()
    assert chdata.start_timestamp < timespan1.end_datetime.timestamp()
    assert chdata.data.size > 0


def test_timespan_channels(store: NCEDCS3DataStore):
    timespans = store.get_timespans()
    assert len(timespans) == 1
    assert timespans[0] == timespan1
    channels = store.get_channels(timespan1)
    assert len(channels) == len(read_channels)
    channels = store.get_channels(timespan2)
    assert len(channels) == 0
