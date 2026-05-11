import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest import mock

import pytest
from datetimerange import DateTimeRange
from utils import date_range

from noisepy.seis.io.datatypes import Channel, ChannelType, Station
from noisepy.seis.io.pnwstore import PNWDataStore
from noisepy.seis.io.stores import (
    StackStore,
    convert_stackstore,
    parse_station_pair,
    parse_timespan,
    timespan_str,
)


class _DummyStackStore(StackStore):
    def __init__(self, values):
        self.values = values

    def contains(self, src, rec, timespan):
        return (src, rec, str(timespan)) in self.values

    def append(self, timespan, src, rec, ccs):
        self.values[(src, rec, str(timespan))] = ccs

    def get_timespans(self, src_sta, rec_sta):
        return []

    def get_station_pairs(self):
        return []

    def read(self, timespan, src_sta, rec_sta):
        return self.values.get((src_sta, rec_sta, str(timespan)), [])


def test_read_bulk_uses_pairs_and_returns_ordered_results():
    ts = date_range(4, 1, 2)
    p1 = (Station("CI", "AAA"), Station("CI", "BBB"))
    p2 = (Station("UW", "CCC"), Station("UW", "DDD"))
    values = {(p1[0], p1[1], str(ts)): ["a"], (p2[0], p2[1], str(ts)): ["b", "c"]}
    store = _DummyStackStore(values)

    results = store.read_bulk(ts, [p1, p2], executor=ThreadPoolExecutor(max_workers=2))

    assert results == [(p1, ["a"]), (p2, ["b", "c"])]


def test_convert_stackstore_skips_append_when_no_stacks():
    src_store = mock.Mock(spec=StackStore)
    rec_store = mock.Mock(spec=StackStore)
    src_store.read.return_value = []
    ts = date_range(4, 1, 2)
    src = Station("CI", "AAA")
    rec = Station("CI", "BBB")

    convert_stackstore(src_store, rec_store, ts, src, rec)

    src_store.read.assert_called_once_with(ts, src, rec)
    rec_store.append.assert_not_called()


def test_parse_station_pair_valid_and_invalid_inputs():
    pair = parse_station_pair("CI.ARV_CI.BAK")
    assert pair is not None
    assert pair[0] == Station("CI", "ARV")
    assert pair[1] == Station("CI", "BAK")

    assert parse_station_pair("CIARV_CI.BAK") is None
    assert parse_station_pair("CI.ARV_CI.B.AK") is None


def test_parse_timespan_and_timespan_str_round_trip():
    ts = date_range(4, 1, 2)
    encoded = timespan_str(ts)
    parsed = parse_timespan(f"/tmp/{encoded}.npy")

    assert parsed is not None
    assert parsed.start_datetime == ts.start_datetime
    assert parsed.end_datetime == ts.end_datetime
    assert parse_timespan("not_a_timespan.npy") is None


def _new_pnw_store(db_file):
    store = PNWDataStore.__new__(PNWDataStore)
    store.db_file = db_file
    return store


def test_pnw_parse_timespan_and_channel_helpers():
    ts = PNWDataStore._parse_timespan("YA2.UW.2020.366")
    assert ts.start_datetime == datetime(2020, 12, 31, tzinfo=timezone.utc)
    assert ts.end_datetime == datetime(2021, 1, 1, tzinfo=timezone.utc)

    ch = PNWDataStore._parse_channel(("UW", "YA2", "BHN", "00", "unused"))
    assert ch.station == Station("UW", "YA2", location="00")
    assert ch.type == ChannelType("BHN", "00")


def test_pnw_dbquery_reads_rows_from_sqlite(tmp_path):
    db_file = str(tmp_path / "tsindex.sqlite")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE sample (id INTEGER)")
    conn.execute("INSERT INTO sample (id) VALUES (1), (2)")
    conn.commit()
    conn.close()

    store = _new_pnw_store(db_file)
    rows = store._dbquery("SELECT id FROM sample ORDER BY id")
    assert rows == [(1,), (2,)]


@pytest.mark.parametrize(
    "db_rows,exists,expected_empty",
    [
        ([], True, True),
        ([(0, 10)] * 11, True, True),
        ([(0, 10)], False, True),
    ],
)
def test_pnw_read_data_guard_paths(tmp_path, db_rows, exists, expected_empty):
    ts = date_range(4, 1, 2)
    chan = Channel(ChannelType("BHN"), Station("UW", "YA2", location="00"))
    store = _new_pnw_store(str(tmp_path / "unused.sqlite"))
    store.paths = {ts.start_datetime: str(tmp_path)}
    store._dbquery = lambda _: db_rows
    store.fs = mock.Mock()
    store.fs.exists.return_value = exists

    data = store.read_data(ts, chan)

    assert (len(data.data) == 0) is expected_empty


def test_pnw_read_data_rejects_cross_year_timespan(tmp_path):
    ts = DateTimeRange(
        datetime(2021, 12, 31, tzinfo=timezone.utc),
        datetime(2022, 1, 1, tzinfo=timezone.utc),
    )
    chan = Channel(ChannelType("BHN"), Station("UW", "YA2", location="00"))
    store = _new_pnw_store(str(tmp_path / "unused.sqlite"))
    store._dbquery = lambda _: []

    with pytest.raises(AssertionError, match="cross years"):
        store.read_data(ts, chan)
