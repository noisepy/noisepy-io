"""
Tests for the array-backed ChannelData and the seisfetch-backed raw store.

The store's network I/O is mocked: what is tested here is the contract the
pre-processing chain depends on — segments in, segments out, no obspy on the
way through, and an obspy Stream still available on demand.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import numpy as np
import pytest
from datetimerange import DateTimeRange

from noisepy.seis.io.datatypes import Channel, ChannelData, ChannelType, Station, TraceSegment
from noisepy.seis.io.seisfetchstore import SeisfetchChannelCatalog, SeisfetchRawDataStore

TS = DateTimeRange(
    datetime(2022, 1, 2, tzinfo=timezone.utc),
    datetime(2022, 1, 3, tzinfo=timezone.utc),
)


class TestTraceSegment:
    def test_end_timestamp_is_last_sample_like_obspy(self):
        import obspy

        data = np.arange(10, dtype=np.float32)
        seg = TraceSegment(data=data, sampling_rate=20.0, start_timestamp=1000.0, id="CI.PASC..BHZ")
        trace = obspy.Trace(data, header={"sampling_rate": 20.0, "starttime": obspy.UTCDateTime(1000.0)})
        assert seg.end_timestamp == trace.stats.endtime.timestamp
        assert seg.npts == trace.stats.npts

    def test_round_trips_through_obspy_trace(self):
        seg = TraceSegment(
            data=np.arange(5, dtype=np.float32), sampling_rate=40.0, start_timestamp=1e9, id="CI.PASC.00.BHZ"
        )
        trace = seg.to_trace()
        assert trace.id == "CI.PASC.00.BHZ"
        back = TraceSegment.from_trace(trace)
        assert back.id == seg.id
        assert back.sampling_rate == seg.sampling_rate
        assert back.start_timestamp == seg.start_timestamp
        np.testing.assert_array_equal(back.data, seg.data)

    def test_empty_segment_end_timestamp(self):
        seg = TraceSegment(data=np.empty(0), sampling_rate=0.0, start_timestamp=5.0)
        assert seg.end_timestamp == 5.0
        assert seg.npts == 0


class TestChannelData:
    def test_from_array(self):
        ch = ChannelData.from_array(np.arange(4.0), 20.0, 100.0, id="CI.PASC..BHZ")
        assert ch.sampling_rate == 20.0
        assert ch.start_timestamp == 100.0
        assert ch.id == "CI.PASC..BHZ"
        assert len(ch.segments) == 1

    def test_from_segments_exposes_first_segment(self):
        segs = [
            TraceSegment(data=np.arange(3.0), sampling_rate=20.0, start_timestamp=100.0, id="X"),
            TraceSegment(data=np.arange(5.0), sampling_rate=20.0, start_timestamp=200.0, id="X"),
        ]
        ch = ChannelData.from_segments(segs)
        assert len(ch.segments) == 2
        np.testing.assert_array_equal(ch.data, segs[0].data)
        assert ch.start_timestamp == 100.0

    def test_empty(self):
        ch = ChannelData.empty()
        assert ch.data.size == 0
        assert ch.sampling_rate == 0.0
        assert ch.id == ""
        # one zero-length segment, mirroring the old Stream([Trace(empty)]):
        # callers that index the first segment/trace keep working
        assert len(ch.segments) == 1
        assert ch.segments[0].npts == 0

    def test_stream_is_built_lazily_and_cached(self):
        ch = ChannelData.from_array(np.arange(4.0), 20.0, 100.0, id="CI.PASC..BHZ")
        assert ch._stream is None
        stream = ch.stream
        assert len(stream) == 1
        assert stream[0].id == "CI.PASC..BHZ"
        assert ch.stream is stream  # cached, not rebuilt

    def test_constructed_from_stream_keeps_that_stream(self):
        import obspy

        trace = obspy.Trace(
            np.arange(6, dtype=np.float32),
            header={"network": "CI", "station": "PASC", "channel": "BHZ", "sampling_rate": 40.0},
        )
        stream = obspy.Stream([trace])
        ch = ChannelData(stream)
        assert ch.stream is stream
        assert ch.sampling_rate == 40.0
        assert len(ch.segments) == 1
        np.testing.assert_array_equal(ch.data, trace.data)

    def test_multi_trace_stream_becomes_multiple_segments(self):
        import obspy

        traces = [
            obspy.Trace(np.arange(4, dtype=np.float32), header={"sampling_rate": 20.0}),
            obspy.Trace(np.arange(3, dtype=np.float32), header={"sampling_rate": 20.0}),
        ]
        ch = ChannelData(obspy.Stream(traces))
        assert len(ch.segments) == 2

    def test_empty_stream_is_tolerated(self):
        import obspy

        ch = ChannelData(obspy.Stream([]))
        assert ch.data.size == 0
        assert ch.segments == []


class _FakeTraceArray:
    """Minimal stand-in for seisfetch.convert.TraceArray."""

    def __init__(self, data, sampling_rate, starttime_ns):
        self.data = data
        self.sampling_rate = sampling_rate
        self.starttime_ns = starttime_ns


class _FakeBundle:
    def __init__(self, segments):
        self._segments = segments

    def segments(self):
        return self._segments


def _store(**kwargs) -> SeisfetchRawDataStore:
    return SeisfetchRawDataStore(
        stations=kwargs.pop("stations", ["CI.PASC"]),
        channels=kwargs.pop("channels", ["BHZ"]),
        date_range=kwargs.pop("date_range", TS),
        chan_catalog=kwargs.pop("chan_catalog", _NoopCatalog()),
        **kwargs,
    )


class _NoopCatalog(SeisfetchChannelCatalog):
    def get_full_channel(self, timespan, channel):
        return Channel(
            channel.type,
            Station(
                channel.station.network, channel.station.name, 34.0, -118.0, 100.0, channel.station.location
            ),
        )


class TestSeisfetchRawDataStore:
    def test_station_spec_parsing(self):
        store = _store(stations=["CI.PASC", "BK.PKD.00"])
        assert store.stations[0].network == "CI"
        assert store.stations[0].location == ""
        assert store.stations[1].location == "00"

    def test_bad_station_spec_raises(self):
        with pytest.raises(ValueError, match="NET.STA"):
            _store(stations=["PASC"])

    def test_station_spec_with_a_channel_appended_raises(self):
        # NET.STA.LOC.CHA would otherwise silently drop the channel and read a
        # different location code than the spec appears to name
        with pytest.raises(ValueError, match="channel codes are passed separately"):
            _store(stations=["CI.PASC.00.BHZ"])

    def test_timespans_are_utc_days(self):
        store = _store(
            date_range=DateTimeRange(datetime(2022, 1, 2), datetime(2022, 1, 5))
        )  # naive -> treated as UTC
        spans = store.get_timespans()
        assert len(spans) == 3
        assert spans[0].start_datetime == datetime(2022, 1, 2, tzinfo=timezone.utc)
        assert spans[-1].end_datetime == datetime(2022, 1, 5, tzinfo=timezone.utc)

    def test_get_channels_populates_coordinates(self):
        store = _store(stations=["CI.PASC"], channels=["BHE", "BHZ"])
        channels = store.get_channels(TS)
        assert len(channels) == 2
        assert all(c.station.valid() for c in channels)

    def test_read_data_returns_segment_backed_channel_data(self):
        store = _store()
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))
        t0_ns = int(TS.start_datetime.timestamp() * 1e9) + 19_537_920
        segs = {"CI.PASC..BHZ": [_FakeTraceArray(np.arange(100, dtype=np.float32), 40.0, t0_ns)]}

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.return_value = b"\x00" * 512
            with patch("seisfetch.convert.parse_mseed", return_value=_FakeBundle(segs)):
                data = store.read_data(TS, chan)

        assert len(data.segments) == 1
        assert data.sampling_rate == 40.0
        # exact nanoseconds preserved through the epoch-seconds conversion
        assert data.start_timestamp == t0_ns / 1e9
        assert data.id == "CI.PASC..BHZ"

    def test_read_data_resolves_a_wildcard_location(self):
        """Blank location in the spec, location-coded data in the archive."""
        store = _store()
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))
        segs = {"CI.PASC.00.BHZ": [_FakeTraceArray(np.arange(10, dtype=np.float32), 40.0, 0)]}

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.return_value = b"\x00" * 512
            with patch("seisfetch.convert.parse_mseed", return_value=_FakeBundle(segs)):
                data = store.read_data(TS, chan)

        assert data.id == "CI.PASC.00.BHZ"

    def test_read_data_refuses_ambiguous_locations(self):
        store = _store()
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))
        segs = {
            "CI.PASC.00.BHZ": [_FakeTraceArray(np.arange(10, dtype=np.float32), 40.0, 0)],
            "CI.PASC.10.BHZ": [_FakeTraceArray(np.arange(10, dtype=np.float32), 40.0, 0)],
        }

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.return_value = b"\x00" * 512
            with patch("seisfetch.convert.parse_mseed", return_value=_FakeBundle(segs)):
                data = store.read_data(TS, chan)

        assert data.data.size == 0

    def test_read_data_with_no_data_anywhere(self):
        store = _store(fdsn_fallback=False)
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.return_value = b""
            data = store.read_data(TS, chan)

        assert data.data.size == 0

    def test_falls_back_to_fdsn_when_archive_is_empty(self):
        store = _store(fdsn_fallback=True)
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))
        segs = {"CI.PASC..BHZ": [_FakeTraceArray(np.arange(10, dtype=np.float32), 40.0, 0)]}

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.return_value = b""
            with patch.object(SeisfetchRawDataStore, "_fdsn_raw", return_value=b"\x00" * 512) as fdsn:
                with patch("seisfetch.convert.parse_mseed", return_value=_FakeBundle(segs)):
                    data = store.read_data(TS, chan)

        fdsn.assert_called_once()
        assert data.data.size == 10

    def test_s3_error_is_not_fatal(self):
        store = _store(fdsn_fallback=False)
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC", 34.0, -118.0, 100.0, ""))

        with patch.object(SeisfetchRawDataStore, "client") as client:
            client.get_raw.side_effect = RuntimeError("S3 is down")
            data = store.read_data(TS, chan)

        assert data.data.size == 0


class TestSeisfetchChannelCatalog:
    def test_get_full_channel_uses_the_text_service(self):
        catalog = SeisfetchChannelCatalog()
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC"))

        class _Epoch:
            latitude, longitude, elevation = 34.5, -118.5, 250.0

        with patch("seisfetch.earth2.channel_metadata", return_value=[_Epoch()]) as meta:
            full = catalog.get_full_channel(TS, chan)
            # cached: a second call must not re-query
            catalog.get_full_channel(TS, chan)

        assert meta.call_count == 1
        assert full.station.lat == 34.5
        assert full.station.lon == -118.5
        assert full.station.elevation == 250.0

    def test_metadata_failure_leaves_the_channel_untouched(self):
        catalog = SeisfetchChannelCatalog()
        chan = Channel(ChannelType("BHZ"), Station("CI", "PASC"))

        with patch("seisfetch.earth2.channel_metadata", side_effect=RuntimeError("no network")):
            full = catalog.get_full_channel(TS, chan)

        assert not full.station.valid()


class TestInventoryProviderFallback:
    """get_inventory maps seisfetch's datacenter to an obspy FDSN provider.

    seisfetch adds archives over time (GeoNet arrived in 0.4.0), so the map can
    fall behind. It must degrade to a warning, not a KeyError.
    """

    def test_known_datacenters_map_to_providers(self):
        from noisepy.seis.io.seisfetchstore import FDSN_PROVIDERS

        with patch("seisfetch.s3.route_network", return_value="scedc"):
            with patch("obspy.clients.fdsn.Client") as client:
                SeisfetchChannelCatalog().get_inventory(TS, Station("CI", "PASC"))
        assert client.call_args[0][0] == FDSN_PROVIDERS["scedc"]

    def test_unmapped_datacenter_falls_back_to_iris(self, caplog):
        with patch("seisfetch.s3.route_network", return_value="a_new_archive"):
            with patch("obspy.clients.fdsn.Client") as client:
                SeisfetchChannelCatalog().get_inventory(TS, Station("XX", "SOMEWHERE"))
        assert client.call_args[0][0] == "IRIS"
        assert "a_new_archive" in caplog.text

    def test_explicit_provider_wins(self):
        with patch("obspy.clients.fdsn.Client") as client:
            SeisfetchChannelCatalog(provider="GEONET").get_inventory(TS, Station("NZ", "WEL"))
        assert client.call_args[0][0] == "GEONET"

    def test_fdsn_failure_returns_an_empty_inventory(self):
        with patch("obspy.clients.fdsn.Client", side_effect=RuntimeError("FDSN down")):
            inv = SeisfetchChannelCatalog().get_inventory(TS, Station("CI", "PASC"))
        assert len(inv) == 0
