import logging

logging.basicConfig(level=logging.INFO)

import os
from datetime import datetime, timezone

import pytest
from datetimerange import DateTimeRange

from noisepy.seis.io.datatypes import Channel
from noisepy.seis.io.h5store import DASH5DataStore
from noisepy.seis.io.stores import RawDataStore

timespan1 = DateTimeRange(
    datetime(2021, 10, 15, 1, 2, 3, tzinfo=timezone.utc), datetime(2021, 10, 15, 3, 2, 3, tzinfo=timezone.utc)
)
timespan2 = DateTimeRange(
    datetime(2021, 10, 15, 1, 2, 3, tzinfo=timezone.utc), datetime(2021, 10, 15, 1, 3, 3, tzinfo=timezone.utc)
)

stores = [
    DASH5DataStore(
        os.path.join(os.path.dirname(__file__), "./das"),
        2,
        [0, 1, 2],
        "%Y-%m-%d-%H-%M-%S.h5",
        "DAS",
        timespan1,
    ),
    DASH5DataStore(
        os.path.join(os.path.dirname(__file__), "./das"), 2, [0, 1, 2], "%Y-%m-%d-%H-%M-%S.h5", "DAS", None
    ),
]


@pytest.mark.parametrize("store", stores)
def test_parse_channel(store: RawDataStore):
    assert str(store._parse_channel(100)) == "DAS.00100.XXZ"


@pytest.mark.parametrize("store", stores)
def test_parse_timespan(store: RawDataStore):
    span = store._parse_timespan("2021-10-15-01-02-03.h5")
    assert span.start_datetime.timestamp() == timespan2.start_datetime.timestamp()
    assert span.end_datetime.timestamp() == timespan2.end_datetime.timestamp()


@pytest.mark.parametrize("store", stores)
def test_read_data(store: RawDataStore):
    channels = store.get_channels(timespan2)
    assert len(channels) == 3
    data = store.read_data(timespan2, channels[0])
    assert data.data.shape == (120,)
