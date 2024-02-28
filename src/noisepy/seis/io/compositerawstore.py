from typing import Dict, List

import obspy
from datetimerange import DateTimeRange

from .datatypes import Channel, ChannelData, Station
from .stores import RawDataStore


class CompositeRawStore(RawDataStore):
    """
    A class for reading the raw data for a given channel from multiple sources
    """

    def __init__(self, stores: Dict[str, RawDataStore]):
        self.stores = stores

    def get_channels(self, timespan: DateTimeRange) -> List[Channel]:
        return [chan for store in self.stores.values() for chan in store.get_channels(timespan)]

    def get_timespans(self) -> List[DateTimeRange]:
        timespans = dict([nets, store.get_timespans()] for nets, store in self.stores.items())
        uniquespans = []
        for netspans in timespans.values():
            for span in netspans:
                if span not in uniquespans:
                    uniquespans.append(span)
        return uniquespans

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        return self._store(chan.station.network).read_data(timespan, chan)

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return self._store(station.network).get_inventory(timespan, station)

    def _store(self, network: str) -> RawDataStore:
        if network not in self.stores:
            raise ValueError(f"Network {network} not found in known stores")
        return self.stores[network]
