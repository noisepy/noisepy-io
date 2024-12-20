import logging
import os
import re
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Callable, List

import obspy
from datetimerange import DateTimeRange

from .channelcatalog import ChannelCatalog
from .datatypes import Channel, ChannelData, ChannelType, Station
from .stores import RawDataStore
from .utils import TimeLogger, fs_join, get_filesystem

logger = logging.getLogger(__name__)


class MiniSeedS3DataStore(RawDataStore):
    """
    A data store implementation to read from a directory of miniSEED (.ms) files from an S3 bucket.
    Every directory is a a day and each .ms file contains the data for a channel.
    """

    def __init__(
        self,
        path: str,
        chan_catalog: ChannelCatalog,
        chan_filter: Callable[[Channel], bool] = lambda s: True,  # noqa: E731
        date_range: DateTimeRange = None,
        file_name_regex: str = None,
        storage_options: dict = {},
    ):
        """
        Parameters:
            path: path to look for ms files. Can be a local file directory or an s3://... url path
            chan_catalog: ChannelCatalog to retrieve inventory information for the channels
            chan_filter: Function to decide whether a channel should be used or not,
                            if None, all channels are used
        """
        super().__init__()
        self.file_re = re.compile(file_name_regex, re.IGNORECASE)
        self.fs = get_filesystem(path, storage_options=storage_options)
        self.chan_catalog = chan_catalog
        self.path = path
        self.paths = {}
        # to store a dict of {timerange: list of channels}
        self.channels = defaultdict(list)
        self.chan_filter = chan_filter
        if date_range is not None and date_range.start_datetime.tzinfo is None:
            start_datetime = date_range.start_datetime.replace(tzinfo=timezone.utc)
            end_datetime = date_range.end_datetime.replace(tzinfo=timezone.utc)
            date_range = DateTimeRange(start_datetime, end_datetime)

        self.date_range = date_range

        if date_range is None:
            self._load_channels(self.path, chan_filter)

    def _load_channels(self, full_path: str, chan_filter: Callable[[Channel], bool]):
        tlog = TimeLogger(logger=logger, level=logging.INFO)
        msfiles = [f for f in self.fs.glob(fs_join(full_path, "*")) if self.file_re.match(f) is not None]
        tlog.log(f"Listing {len(msfiles)} files from {full_path}")
        for f in msfiles:
            timespan = self._parse_timespan(f)
            self.paths[timespan.start_datetime] = full_path
            channel = self._parse_channel(os.path.basename(f))
            if not chan_filter(channel):
                continue
            key = str(timespan)  # DataTimeFrame is not hashable
            self.channels[key].append(channel)
        tlog.log(
            f"Init: {len(self.channels)} timespans and {sum(len(ch) for ch in  self.channels.values())} channels"
        )

    def _ensure_channels_loaded(self, date_range: DateTimeRange):
        key = str(date_range)
        if key not in self.channels or date_range.start_datetime not in self.paths:
            dt = date_range.end_datetime - date_range.start_datetime
            for d in range(0, dt.days):
                date = date_range.start_datetime + timedelta(days=d)
                if self.date_range is None or date not in self.date_range:
                    continue
                date_path = self._get_datepath(date)
                full_path = fs_join(self.path, date_path)
                self._load_channels(full_path, self.chan_filter)

    def get_channels(self, date_range: DateTimeRange) -> List[Channel]:
        self._ensure_channels_loaded(date_range)
        tmp_channels = self.channels.get(str(date_range), [])
        executor = ThreadPoolExecutor()
        stations = set(map(lambda c: c.station, tmp_channels))
        _ = list(executor.map(lambda s: self.chan_catalog.get_inventory(date_range, s), stations))
        logger.info(f"Getting {len(tmp_channels)} channels for {date_range}")
        return list(executor.map(lambda c: self.chan_catalog.get_full_channel(date_range, c), tmp_channels))

    def get_timespans(self) -> List[DateTimeRange]:
        if self.date_range is not None:
            days = (self.date_range.end_datetime - self.date_range.start_datetime).days
            return [
                DateTimeRange(
                    self.date_range.start_datetime.replace(tzinfo=timezone.utc) + timedelta(days=d),
                    self.date_range.start_datetime.replace(tzinfo=timezone.utc) + timedelta(days=d + 1),
                )
                for d in range(0, days)
            ]
        return list([DateTimeRange.from_range_text(d) for d in sorted(self.channels.keys())])

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        self._ensure_channels_loaded(timespan)
        # reconstruct the file name from the channel parameters
        filename = self._get_filename(timespan, chan)
        if not self.fs.exists(filename):
            logger.warning(f"Could not find file {filename}")
            return ChannelData.empty()

        with self.fs.open(filename) as f:
            stream = obspy.read(f)
        data = ChannelData(stream)
        return data

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return self.chan_catalog.get_inventory(timespan, station)

    @abstractmethod
    def _get_datepath(self, timespan: datetime) -> str:
        pass

    @abstractmethod
    def _get_filename(self, timespan: DateTimeRange, channel: Channel) -> str:
        pass

    @abstractmethod
    def _parse_channel(self, filename: str) -> Channel:
        pass

    @abstractmethod
    def _parse_timespan(self, filename: str) -> DateTimeRange:
        pass


class SCEDCS3DataStore(MiniSeedS3DataStore):
    def __init__(
        self,
        path: str,
        chan_catalog: ChannelCatalog,
        chan_filter: Callable[[Channel], bool] = lambda s: True,  # noqa: E731
        date_range: DateTimeRange = None,
        storage_options: dict = {},
    ):
        super().__init__(
            path,
            chan_catalog,
            chan_filter=chan_filter,
            date_range=date_range,
            # for checking the filename has the form: CIGMR__LHN___2022002.ms
            file_name_regex=r".*[0-9]{7}\.ms$",
            storage_options=storage_options,
        )

    def _parse_channel(self, filename: str) -> Channel:
        # e.g.
        # CIGMR__LHN___2022002
        # CE13884HNZ10_2022002
        network = filename[:2]
        station = filename[2:7].rstrip("_")
        channel = filename[7:10]
        location = filename[10:12].strip("_")
        return Channel(
            ChannelType(channel, location),
            # lat/lon/elev will be populated later
            Station(network, station, location=location),
        )

    def _parse_timespan(self, filename: str) -> DateTimeRange:
        # The SCEDC S3 bucket stores files in the form: CIGMR__LHN___2022002.ms
        year = int(filename[-10:-6])
        day = int(filename[-6:-3])
        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        return DateTimeRange(jan1 + timedelta(days=day - 1), jan1 + timedelta(days=day))

    def _get_filename(self, timespan: DateTimeRange, channel: Channel) -> str:
        chan_str = (
            f"{channel.station.network}{channel.station.name.ljust(5, '_')}{channel.type.name}"
            f"{channel.station.location.ljust(3, '_')}"
        )
        filename = fs_join(
            self.paths[timespan.start_datetime], f"{chan_str}{timespan.start_datetime.strftime('%Y%j')}.ms"
        )
        return filename

    def _get_datepath(self, date: datetime) -> str:
        return str(date.year) + "/" + str(date.year) + "_" + str(date.timetuple().tm_yday).zfill(3) + "/"


class NCEDCS3DataStore(MiniSeedS3DataStore):
    def __init__(
        self,
        path: str,
        chan_catalog: ChannelCatalog,
        chan_filter: Callable[[Channel], bool] = lambda s: True,  # noqa: E731
        date_range: DateTimeRange = None,
        storage_options: dict = {},
    ):
        super().__init__(
            path,
            chan_catalog,
            chan_filter=chan_filter,
            date_range=date_range,
            # for checking the filename has the form: AAS.NC.EHZ..D.2020.002
            file_name_regex=r".*[0-9]{4}.*[0-9]{3}$",
            storage_options=storage_options,
        )

    def _parse_channel(self, filename: str) -> Channel:
        # e.g.
        # AAS.NC.EHZ..D.2020.002
        split_fn = filename.split(".")
        network = split_fn[1]
        station = split_fn[0]
        channel = split_fn[2]
        location = split_fn[3]
        if len(channel) > 3:
            channel = channel[:3]
        return Channel(
            ChannelType(channel, location),
            # lat/lon/elev will be populated later
            Station(network, station, location=location),
        )

    def _parse_timespan(self, filename: str) -> DateTimeRange:
        # The NCEDC S3 bucket stores files in the form: AAS.NC.EHZ..D.2020.002
        year = int(filename[-8:-4])
        day = int(filename[-3:])
        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        return DateTimeRange(jan1 + timedelta(days=day - 1), jan1 + timedelta(days=day))

    def _get_datepath(self, date: datetime) -> str:
        return str(date.year) + "/" + str(date.year) + "." + str(date.timetuple().tm_yday).zfill(3) + "/"

    def _get_filename(self, timespan: DateTimeRange, chan: Channel) -> str:
        chan_str = (
            f"{chan.station.name}.{chan.station.network}.{chan.type.name}." f"{chan.station.location}.D"
        )
        return fs_join(
            self.paths[timespan.start_datetime], f"{chan_str}.{timespan.start_datetime.strftime('%Y.%j')}"
        )
