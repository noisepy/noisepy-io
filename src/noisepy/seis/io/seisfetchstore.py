"""
Raw data store backed by seisfetch — the obspy-free input path.

Two things distinguish this store from :mod:`noisepy.seis.io.s3store`:

1. **No data-center knowledge lives here.** Bucket names, key layouts and the
   network -> archive routing all come from :mod:`seisfetch.s3`
   (``route_network``), which is the single owner of that knowledge. Adding an
   archive is a seisfetch change, not a noisepy-io change.
2. **No obspy.** miniSEED is decoded by pymseed via ``seisfetch.parse_mseed``
   into :class:`~noisepy.seis.io.datatypes.TraceSegment` arrays, and station
   coordinates come from the FDSN station *text* service (one HTTP request,
   stdlib parsing) rather than StationXML.

``get_inventory`` is the one method that still needs obspy: it is called only
on the ``rm_resp != NO`` pre-processing path, so a ``rm_resp=NO`` pipeline runs
without obspy installed at all.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable, List, Optional

from datetimerange import DateTimeRange

from .channelcatalog import ChannelCatalog
from .datatypes import Channel, ChannelData, ChannelType, Station, TraceSegment
from .stores import RawDataStore

if TYPE_CHECKING:  # pragma: no cover - typing only
    import obspy

logger = logging.getLogger(__name__)

# seisfetch datacenter -> obspy FDSN provider key, for the StationXML path only
FDSN_PROVIDERS = {
    "scedc": "SCEDC",
    "ncedc": "NCEDC",
    "geonet": "GEONET",
    "earthscope": "IRIS",
}


def _to_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


class SeisfetchChannelCatalog(ChannelCatalog):
    """
    Channel metadata from the FDSN station text service — no StationXML, no obspy.

    ``level=channel&format=text`` returns coordinates, elevation and the total
    sensitivity in a single request; :func:`seisfetch.earth2.channel_metadata`
    parses it with the stdlib. The provider is chosen from the same routing
    table that selects the S3 archive, so metadata and waveforms agree.
    """

    def __init__(self, provider: Optional[str] = None):
        super().__init__()
        self.provider = provider
        self._cache: dict = {}

    def _epochs(self, timespan: DateTimeRange, ch: Channel) -> list:
        from seisfetch.earth2 import channel_metadata

        key = (ch.station.network, ch.station.name, ch.station.location, ch.type.name, str(timespan))
        if key not in self._cache:
            try:
                self._cache[key] = channel_metadata(
                    ch.station.network,
                    ch.station.name,
                    ch.station.location,
                    ch.type.name,
                    _to_utc(timespan.start_datetime),
                    _to_utc(timespan.end_datetime),
                    provider=self.provider,
                )
            except Exception as e:
                logger.warning(f"Could not get channel metadata for {ch}: {e}")
                self._cache[key] = []
        return self._cache[key]

    def get_full_channel(self, timespan: DateTimeRange, channel: Channel) -> Channel:
        """Populate lat/lon/elevation without building an Inventory."""
        epochs = self._epochs(timespan, channel)
        if not epochs:
            return channel
        e = epochs[0]
        return Channel(
            channel.type,
            Station(
                network=channel.station.network,
                name=channel.station.name,
                lat=e.latitude,
                lon=e.longitude,
                elevation=e.elevation,
                location=channel.station.location,
            ),
        )

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> "obspy.Inventory":
        """
        StationXML inventory — needed only for ``rm_resp != NO``.

        Requires the ``obspy`` extra. The rest of this catalog (and the whole
        ``rm_resp=NO`` path) never calls it.
        """
        import obspy
        from obspy.clients.fdsn import Client
        from seisfetch.s3 import route_network

        # seisfetch grows archives over time (GeoNet arrived in 0.4.0), so an
        # unmapped datacenter is a question of when, not if. Fall back to the
        # federated IRIS service with a warning rather than raising KeyError:
        # the caller wants metadata, and IRIS serves most of it.
        provider = self.provider
        if provider is None:
            datacenter = route_network(station.network)
            provider = FDSN_PROVIDERS.get(datacenter)
            if provider is None:
                logger.warning(
                    f"No FDSN provider mapped for seisfetch datacenter '{datacenter}'; "
                    "falling back to IRIS. Pass provider= to choose explicitly."
                )
                provider = "IRIS"
        try:
            return Client(provider).get_stations(
                network=station.network,
                station=station.name,
                starttime=obspy.UTCDateTime(_to_utc(timespan.start_datetime)),
                endtime=obspy.UTCDateTime(_to_utc(timespan.end_datetime)),
                level="response",
            )
        except Exception as e:
            logger.warning(f"Could not get inventory for {station}: {e}")
            return obspy.Inventory()


class SeisfetchRawDataStore(RawDataStore):
    """
    Read channel-days from any archive seisfetch routes to (SCEDC, NCEDC,
    EarthScope, GeoNet) or, when the archive has no object for the request,
    from that network's FDSN service.

    Parameters:
        stations: station specs as ``NET.STA`` or ``NET.STA.LOC``, e.g. ``CI.PASC.00``
        channels: channel codes to read for every station, e.g. ``["BHE", "BHN", "BHZ"]``
        date_range: timespans are the UTC days inside this range
        chan_catalog: catalog used to fill coordinates; defaults to the
            obspy-free :class:`SeisfetchChannelCatalog`
        chan_filter: optional predicate to drop channels
        fdsn_fallback: try FDSN when the S3 archive has no data for a channel-day
    """

    def __init__(
        self,
        stations: List[str],
        channels: List[str],
        date_range: DateTimeRange,
        chan_catalog: Optional[ChannelCatalog] = None,
        chan_filter: Callable[[Channel], bool] = lambda c: True,  # noqa: E731
        fdsn_fallback: bool = True,
        max_workers: int = 8,
    ):
        super().__init__()
        self.date_range = DateTimeRange(_to_utc(date_range.start_datetime), _to_utc(date_range.end_datetime))
        self.channels_codes = list(channels)
        self.chan_catalog = chan_catalog if chan_catalog is not None else SeisfetchChannelCatalog()
        self.chan_filter = chan_filter
        self.fdsn_fallback = fdsn_fallback
        self.max_workers = max_workers
        self._client = None
        self.stations = []
        for spec in stations:
            parts = spec.split(".")
            if len(parts) < 2 or len(parts) > 3:
                # silently ignoring extra fields would read a different
                # location code than the spec appears to name; a channel
                # accidentally appended here belongs in `channels`
                raise ValueError(
                    f"Station spec '{spec}' must be NET.STA or NET.STA.LOC "
                    "(channel codes are passed separately, via `channels`)"
                )
            net, sta = parts[0], parts[1]
            loc = parts[2] if len(parts) > 2 else ""
            self.stations.append(Station(network=net, name=sta, location=loc))

    @property
    def client(self):
        if self._client is None:
            from seisfetch.s3 import S3OpenClient

            self._client = S3OpenClient(max_workers=self.max_workers)
        return self._client

    def get_timespans(self) -> List[DateTimeRange]:
        days = (self.date_range.end_datetime - self.date_range.start_datetime).days
        return [
            DateTimeRange(
                self.date_range.start_datetime + timedelta(days=d),
                self.date_range.start_datetime + timedelta(days=d + 1),
            )
            for d in range(0, days)
        ]

    def get_channels(self, timespan: DateTimeRange) -> List[Channel]:
        chans = [
            Channel(ChannelType(code, sta.location), sta)
            for sta in self.stations
            for code in self.channels_codes
        ]
        chans = [c for c in chans if self.chan_filter(c)]
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            full = list(executor.map(lambda c: self.chan_catalog.get_full_channel(timespan, c), chans))
        logger.info(f"Getting {len(full)} channels for {timespan}")
        return full

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        from seisfetch.convert import parse_mseed

        start = _to_utc(timespan.start_datetime)
        end = _to_utc(timespan.end_datetime)
        location = chan.type.location or chan.station.location or ""

        # blank location means "whichever location code this station uses" —
        # seisfetch resolves the wildcard by a paginated LIST per station-day
        s3_location = location if location else "*"

        raw = b""
        try:
            raw = self.client.get_raw(
                chan.station.network,
                chan.station.name,
                start.timestamp(),
                end.timestamp(),
                location=s3_location,
                channel=chan.type.name,
                missing_ok=True,
            )
        except Exception as e:
            logger.warning(f"S3 read failed for {chan} {timespan}: {e}")

        if not raw and self.fdsn_fallback:
            raw = self._fdsn_raw(chan, start, end, s3_location)

        if not raw:
            logger.warning(f"No data for {chan} {timespan}")
            return ChannelData.empty()

        # NOT trimmed to the window here: the obspy path reads whole day
        # files and trims once at the END of pre-processing (nearest-sample,
        # pad with zeros). Trimming now (inside-window) would drop different
        # samples before detrend/taper/filter and the two paths would diverge
        # for windows that are off the sample grid.
        bundle = parse_mseed(raw)
        all_segments = bundle.segments()
        nslc = f"{chan.station.network}.{chan.station.name}.{location}.{chan.type.name}"
        segments = all_segments.get(nslc, [])
        if not segments:
            # A blank/wildcard location matches whatever code the archive
            # carries; match on net/sta/cha and take the single hit. More than
            # one location code is ambiguous, so name it rather than guess.
            matches = {
                k: v
                for k, v in all_segments.items()
                if k.split(".")[0] == chan.station.network
                and k.split(".")[1] == chan.station.name
                and k.split(".")[3] == chan.type.name
            }
            if len(matches) == 1:
                nslc, segments = next(iter(matches.items()))
            else:
                logger.warning(
                    f"{nslc} not resolvable among parsed streams {list(all_segments)}; "
                    "specify the location code in the station spec (NET.STA.LOC)"
                )
                return ChannelData.empty()

        return ChannelData.from_segments(
            [
                TraceSegment(
                    data=s.data,
                    sampling_rate=float(s.sampling_rate),
                    start_timestamp=s.starttime_ns / 1e9,
                    id=nslc,
                )
                for s in segments
            ]
        )

    def _fdsn_raw(self, chan: Channel, start: datetime, end: datetime, location: str) -> bytes:
        try:
            from seisfetch.fdsn import FDSNMultiClient

            return FDSNMultiClient().get_raw(
                chan.station.network,
                chan.station.name,
                location=location or "*",
                channel=chan.type.name,
                starttime=start,
                endtime=end,
            )
        except Exception as e:
            logger.warning(f"FDSN fallback failed for {chan}: {e}")
            return b""

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> "obspy.Inventory":
        return self.chan_catalog.get_inventory(timespan, station)
