import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List

import h5py
import obspy
from datetimerange import DateTimeRange

from .datatypes import Channel, ChannelData, ChannelType, Station
from .utils import TimeLogger, fs_join, get_filesystem

logger = logging.getLogger(__name__)


class DASH5DataStore:
    """
    A data store implementation to read from a directory of HDF5 (.h5) files.
    Every file is a minute recording and each .h5 file contains the data for all channel.
    """

    def __init__(
        self,
        path: str,
        sampling_rate: int,
        channel_numbers: List[int],
        file_naming: callable,
        array_name: str = "DAS",
        date_range: DateTimeRange = None,
        storage_options: dict = {},
    ):
        """
        Parameters:
            path: path to look for ms files. Can be a local file directory or an s3://... url path
            chan_catalog: ChannelCatalog to retrieve inventory information for the channels
            channel_filter: Function to decide whether a channel should be used or not,
                            if None, all channels are used
        """
        super().__init__()
        self.fs = get_filesystem(path, storage_options=storage_options)
        self.sampling_rate = sampling_rate
        self.array_name = array_name
        self.channel_numbers = channel_numbers
        self.file_naming = file_naming
        self.path = path
        self.paths = {}
        self.channels = defaultdict(list)
        if date_range is not None and date_range.start_datetime.tzinfo is None:
            start_datetime = date_range.start_datetime.replace(tzinfo=timezone.utc)
            end_datetime = date_range.end_datetime.replace(tzinfo=timezone.utc)
            date_range = DateTimeRange(start_datetime, end_datetime)

        self.date_range = date_range

        if date_range is None:
            self._load_channels(self.path)

    def _load_channels(self, full_path: str):
        tlog = TimeLogger(logger=logger, level=logging.INFO)
        msfiles = [f for f in self.fs.glob(full_path)]
        tlog.log(f"Loading {len(msfiles)} files from {full_path}")
        for f in msfiles:
            timespan = self._parse_timespan(os.path.basename(f))
            self.paths[timespan.start_datetime] = full_path
            for idx in self.channel_numbers:
                channel = self._parse_array(idx)
                key = str(timespan)  # DataTimeFrame is not hashable
                self.channels[key].append(channel)
        tlog.log(
            f"Init: {len(self.channels)} timespans and {sum(len(ch) for ch in  self.channels.values())} channels"
        )

    def _ensure_array_loaded(self, date_range: DateTimeRange):
        key = str(date_range)
        if key not in self.channels or date_range.start_datetime not in self.paths:
            dt = date_range.end_datetime - date_range.start_datetime
            for d in range(0, dt.seconds // 60):
                date = date_range.start_datetime + timedelta(minutes=d)
                if self.date_range is None or date not in self.date_range:
                    continue
                date_path = self._get_datepath(date)
                full_path = fs_join(self.path, date_path)
                self._load_channels(full_path)

    def get_channels(self, date_range: DateTimeRange) -> List[Channel]:
        self._ensure_array_loaded(date_range)
        tmp_channels = self.channels.get(str(date_range), [])
        stations = set(map(lambda c: c.station, tmp_channels))
        _ = list(map(lambda s: self._validate_station(s), stations))
        logger.info(f"Getting {len(tmp_channels)} channels for {date_range}")
        return tmp_channels

    def get_timespans(self) -> List[DateTimeRange]:
        if self.date_range is not None:
            minutes = (self.date_range.end_datetime - self.date_range.start_datetime).seconds // 60
            return [
                DateTimeRange(
                    self.date_range.start_datetime.replace(tzinfo=timezone.utc) + timedelta(minutes=d),
                    self.date_range.start_datetime.replace(tzinfo=timezone.utc) + timedelta(minutes=d + 1),
                )
                for d in range(0, minutes)
            ]
        return list([DateTimeRange.from_range_text(d) for d in sorted(self.channels.keys())])

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        self._ensure_array_loaded(timespan)
        # reconstruct the file name from the channel parameters
        filename = self._get_filename(timespan)
        number = int(chan.station.name)
        if not self.fs.exists(filename):
            logger.warning(f"Could not find file {filename}")
            return ChannelData.empty()

        with h5py.File(filename, "r") as f:
            data = f["/Acquisition/Raw[0]/RawData"][:, number]
            starttime = f["/Acquisition/Raw[0]/RawDataTime"][0] / 1e6
            trace = obspy.Trace(data)
            trace.stats.network = chan.station.network
            trace.stats.station = chan.station.name
            trace.stats.channel = chan.type.name
            trace.stats.sampling_rate = self.sampling_rate
            trace.stats.starttime = starttime  # no need to fill end time: read only
            stream = obspy.Stream([trace])
        data = ChannelData(stream)
        return data

    def _parse_array(self, cha_numebr: int) -> Channel:
        cha_numebr = str(cha_numebr).zfill(5)
        return Channel(
            ChannelType("XXZ"),
            Station(self.array_name, cha_numebr),
        )

    def _parse_timespan(self, filename: str) -> DateTimeRange:
        # e.g., decimator2_2023-07-03_07.30.08_UTC.h5
        year = int(filename[-26:-22])
        month = int(filename[-21:-19])
        day = int(filename[-18:-16])
        hour = int(filename[-15:-13])
        minute = int(filename[-12:-10])
        second = int(filename[-9:-7])
        starttime = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        return DateTimeRange(starttime, starttime + timedelta(minutes=1))  # one minute file

    def _get_filename(self, timespan: DateTimeRange) -> str:
        filename = self.paths[timespan.start_datetime]
        return filename

    def _get_datepath(self, date: datetime) -> str:
        return fs_join(self.path, self.file_naming(date))

    def _validate_station(self, station: Station):
        station.lat = 0.0
        station.lon = 0.0
        station.elevation = 0.0

    def get_inventory(self, ts, station) -> obspy.Inventory:
        # return an empty inventory
        return obspy.Inventory()
