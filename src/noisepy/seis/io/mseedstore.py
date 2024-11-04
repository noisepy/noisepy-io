import glob
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Callable, List

import obspy
from datetimerange import DateTimeRange

from .channelcatalog import ChannelCatalog
from .datatypes import Channel, ChannelData, ChannelType, Station
from .stores import RawDataStore
from .utils import fs_join, get_filesystem

logger = logging.getLogger(__name__)


class MiniSeedDataStore(RawDataStore):
    """
    A data store implementation to read from a directory of miniSEED data files
    """

    def __init__(
        self,
        path: str,
        date_range: DateTimeRange,
        chan_catalog: ChannelCatalog,
        chan_filter: Callable[[Channel], bool] = None,
    ):
        """
        Parameters:
            path: path to look for ms files. Should be a local file directory
            date_range: Optional date range to filter the data
            chan_catalog: ChannelCatalog to retrieve inventory information for the channels
            chan_filter: Optional function to decide whether a channel should be used or not,
                            if None, all channels are used
        """
        super().__init__()
        self.fs = get_filesystem(path)
        self.chan_catalog = chan_catalog
        self.path = os.path.abspath(path)
        self.paths = {}
        # to store a dict of {timerange: list of channels}
        self.channels = {}
        if chan_filter is None:
            chan_filter = lambda s: True  # noqa: E731

        dt = date_range.end_datetime - date_range.start_datetime
        for d in range(0, dt.days):
            date = date_range.start_datetime + timedelta(days=d)
            self._load_channels(self.get_path(date), chan_filter)

    def _load_channels(self, full_path: str, chan_filter: Callable[[Channel], bool]):
        # The path should look like: $DATA/YYYY/DOY
        year, doy = full_path.split(os.path.sep)[-2:]

        for i in glob.glob(fs_join(full_path, "*")):
            timespan = MiniSeedDataStore._parse_timespan(int(year), int(doy))
            key = str(timespan)
            self.paths[timespan.start_datetime] = full_path
            stream = obspy.read(i, headonly=True)
            ids = set([t.id for t in stream])
            for id in ids:
                channel = MiniSeedDataStore._parse_channel(id)
                if not chan_filter(channel):
                    continue
                if key not in self.channels:
                    self.channels[key] = [channel]
                else:
                    self.channels[key].append(channel)

    def get_path(self, date: datetime) -> str:
        return fs_join(self.path, fs_join(str(date.year), str(date.day).zfill(3)))

    def get_channels(self, timespan: DateTimeRange) -> List[Channel]:
        tmp_channels = self.channels.get(str(timespan), [])
        return list(map(lambda c: self.chan_catalog.get_full_channel(timespan, c), tmp_channels))

    def get_timespans(self) -> List[DateTimeRange]:
        return list([DateTimeRange.from_range_text(d) for d in sorted(self.channels.keys())])

    def get_filename(self, timespan: DateTimeRange, chan: Channel) -> str:
        year = timespan.start_datetime.year
        doy = str(timespan.start_datetime.timetuple().tm_yday).zfill(3)
        net = chan.station.network
        sta = chan.station.name

        return f"{sta}.{net}.{year}.{doy}"

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        assert (
            timespan.start_datetime.year == timespan.end_datetime.year
        ), "Did not expect timespans to cross years"

        filename = fs_join(self.paths[timespan.start_datetime], self.get_filename(timespan, chan))

        # low performance if the file contains all channels
        stream = obspy.read(filename).select(channel=chan.type.name)

        return ChannelData(stream)

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return self.chan_catalog.get_inventory(timespan, station)

    def _parse_timespan(year: int, doy: int) -> DateTimeRange:
        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        return DateTimeRange(jan1 + timedelta(days=doy - 1), jan1 + timedelta(days=doy))

    def _parse_channel(record: str) -> Channel:
        net, sta, loc, cha = record.split(".")

        c = Channel(
            ChannelType(cha, loc),
            # lat/lon/elev will be populated later
            Station(net, sta, location=loc),
        )
        return c
