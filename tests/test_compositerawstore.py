from datetime import datetime, timezone
from unittest.mock import MagicMock

import obspy
import pytest
from datetimerange import DateTimeRange

from noisepy.seis.io.compositerawstore import CompositeRawStore
from noisepy.seis.io.datatypes import Channel, ChannelData, ChannelType, Station


def _timespan(start_day, end_day):
    return DateTimeRange(
        datetime(2022, 1, start_day, tzinfo=timezone.utc),
        datetime(2022, 1, end_day, tzinfo=timezone.utc),
    )


def _channel(network, station_name, channel_name="LHZ"):
    return Channel(ChannelType(channel_name), Station(network, station_name))


def _mock_store(channels, timespans):
    store = MagicMock()
    store.get_channels.return_value = channels
    store.get_timespans.return_value = timespans
    store.read_data.return_value = MagicMock(spec=ChannelData)
    store.get_inventory.return_value = obspy.Inventory()
    return store


@pytest.fixture
def span():
    return _timespan(1, 2)


@pytest.fixture
def ci_store(span):
    return _mock_store([_channel("CI", "WBM")], [span])


@pytest.fixture
def bk_store(span):
    return _mock_store([_channel("BK", "THIS")], [span])


@pytest.fixture
def composite(ci_store, bk_store):
    return CompositeRawStore({"CI": ci_store, "BK": bk_store})


def test_get_channels_aggregates(composite, span):
    channels = composite.get_channels(span)
    assert len(channels) == 2
    assert {ch.station.network for ch in channels} == {"CI", "BK"}


def test_get_timespans_deduplicates(composite, span):
    # Both stores return the same timespan — result should be deduplicated
    spans = composite.get_timespans()
    assert len(spans) == 1
    assert spans[0] == span


def test_get_timespans_multiple(ci_store, span):
    span2 = _timespan(2, 3)
    bk_store = _mock_store([_channel("BK", "THIS")], [span2])
    store = CompositeRawStore({"CI": ci_store, "BK": bk_store})
    spans = store.get_timespans()
    assert len(spans) == 2
    assert span in spans and span2 in spans


def test_read_data_routes_by_network(composite, ci_store, bk_store, span):
    ci_chan = _channel("CI", "WBM")
    bk_chan = _channel("BK", "THIS")

    composite.read_data(span, ci_chan)
    ci_store.read_data.assert_called_once_with(span, ci_chan)
    bk_store.read_data.assert_not_called()

    composite.read_data(span, bk_chan)
    bk_store.read_data.assert_called_once_with(span, bk_chan)


def test_get_inventory_routes_by_network(composite, ci_store, bk_store, span):
    ci_sta = Station("CI", "WBM")
    bk_sta = Station("BK", "THIS")

    composite.get_inventory(span, ci_sta)
    ci_store.get_inventory.assert_called_once_with(span, ci_sta)
    bk_store.get_inventory.assert_not_called()

    composite.get_inventory(span, bk_sta)
    bk_store.get_inventory.assert_called_once_with(span, bk_sta)


def test_unknown_network_raises(composite, span):
    unknown_chan = _channel("XX", "UNKN")
    with pytest.raises(ValueError, match="XX"):
        composite.read_data(span, unknown_chan)


def test_empty_stores(span):
    store = CompositeRawStore({})
    assert store.get_channels(span) == []
    assert store.get_timespans() == []
