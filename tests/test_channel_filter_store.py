import os

from test_channelcatalog import MockCatalog
from test_scedc_s3store import timespan1

from noisepy.seis.io.channel_filter_store import LocationChannelFilterStore, channel_filter
from noisepy.seis.io.datatypes import Channel, ChannelType, Station
from noisepy.seis.io.s3store import SCEDCS3DataStore


def test_location_filtering():
    # This folder has 4 .ms files, 2 of which are the same channel, different location
    path = os.path.join(os.path.dirname(__file__), "./data/scedc/2022/2022_002/")
    store = SCEDCS3DataStore(path, MockCatalog())
    channels = store.get_channels(timespan1)
    assert len(channels) == 4
    filter_store = LocationChannelFilterStore(store)
    # This should filter out the BKTHIS_LHZ10 channel and leave the BKTHIS_LHZ00 channel
    channels = filter_store.get_channels(timespan1)
    assert len(channels) == 3
    bkthis_chan = next(filter(lambda c: c.station.network == "BK", channels))
    assert bkthis_chan.type.location == "00"


def test_filter():
    # filter for station 'staX' or 'staY' and channel type starts with 'B'
    f = channel_filter(["BK"], ["staX", "staY"], ["BHE", "BBB"])
    staX = Station("BK", "staX")
    staZ = Station("BK", "staZ")

    def check(sta, ch_name):
        ch = Channel(ChannelType((ch_name)), sta)
        return f(ch)

    assert check(staX, "BHE") is True
    assert check(staX, "BBB") is True
    assert check(staX, "CHE") is False  # invalid channel name
    assert check(staZ, "BHE") is False  # invalid station
    assert check(staZ, "CHE") is False  # invalid station and channel name
