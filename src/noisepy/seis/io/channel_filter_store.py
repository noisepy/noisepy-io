from typing import Callable, List

import obspy
from datetimerange import DateTimeRange

from noisepy.seis.io.constants import WILD_CARD
from noisepy.seis.io.datatypes import Channel, ChannelData, Station
from noisepy.seis.io.stores import RawDataStore

from .constants import WILD_CARD


class LocationChannelFilterStore(RawDataStore):
    """
    This 'store' simply wraps another store and filters out duplicate channels that differ only
    by location. It does this by keeping the channel with the 'lowest' (lexicographic) location code.
    """

    def __init__(self, store: RawDataStore):
        super().__init__()
        self.store = store

    def get_timespans(self) -> List[DateTimeRange]:
        return self.store.get_timespans()

    def read_data(self, timespan: DateTimeRange, chan: Channel) -> ChannelData:
        return self.store.read_data(timespan, chan)

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return self.store.get_inventory(timespan, station)

    def get_channels(self, timespan: DateTimeRange) -> List[Channel]:
        channels = self.store.get_channels(timespan)
        min_chans = {}
        for ch in channels:
            key = f"{ch.station.network}_{ch.station.name}_{ch.type.name}"
            if key not in min_chans:
                min_chans[key] = ch
            # lexicographic comparison of location codes
            #  http://docs.python.org/reference/expressions.html
            elif ch.type.location < min_chans[key].type.location:
                min_chans[key] = ch
        return list(min_chans.values())


def channel_filter(
    net_list: List[str], sta_list: List[str], cha_list: List[str]
) -> Callable[[Channel], bool]:
    stations = set(sta_list)
    networks = set(net_list)
    channels = set(cha_list)

    def filter(ch: Channel) -> bool:
        return (
            (WILD_CARD in stations or ch.station.name in stations)
            and (WILD_CARD in networks or ch.station.network in networks)
            and (WILD_CARD in channels or ch.type.name in channels)
        )

    return filter
