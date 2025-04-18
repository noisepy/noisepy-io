import os

import obspy
import pandas as pd
import pytest
from datetimerange import DateTimeRange
from obspy import UTCDateTime

from noisepy.seis.io.channelcatalog import (
    ChannelCatalog,
    CSVChannelCatalog,
    FDSNChannelCatalog,
    XMLStationChannelCatalog,
    stats2inv_mseed,
)
from noisepy.seis.io.datatypes import Channel, ChannelType, Station

chan_data = [("ARV", "BHE", 35.1269, -118.83009, 258.0), ("BAK", "BHZ", 35.34444, -119.10445, 116.0)]

file = os.path.join(os.path.dirname(__file__), "./data/station.csv")


@pytest.mark.parametrize("stat,ch,lat,lon,elev", chan_data)
def test_CSVChannelCatalog(stat: str, ch: str, lat: float, lon: float, elev: float):
    cat = CSVChannelCatalog(file)
    chan = Channel(ChannelType(ch), Station("CI", stat))
    full_ch = cat.get_full_channel(DateTimeRange(), chan)
    assert full_ch.station.lat == lat
    assert full_ch.station.lon == lon
    assert full_ch.station.elevation == elev


class MockCatalog(ChannelCatalog):
    def get_full_channel(self, timespan: DateTimeRange, channel: Channel) -> Channel:
        return channel

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return obspy.Inventory()


@pytest.mark.parametrize("station,ch,lat,lon,elev", chan_data)
def test_frominventory(station: str, ch: str, lat: float, lon: float, elev: float):
    file = os.path.join(os.path.dirname(__file__), "./data/station.csv")
    df = pd.read_csv(file)

    class MockStat:
        station = ""
        starttime = UTCDateTime()
        channel = ch
        sampling_rate = 1.0
        location = "00"

    stat = MockStat()
    stat.station = station

    inv = stats2inv_mseed(stat, df)
    cat = MockCatalog()
    chan = Channel(ChannelType(ch), Station("CI", station))
    full_ch = cat.populate_from_inventory(inv, chan)
    assert full_ch.station.lat == lat
    assert full_ch.station.lon == lon
    assert full_ch.station.elevation == elev


xmlpaths = [os.path.join(os.path.dirname(__file__), "./data/CI/"), "s3://scedc-pds/FDSNstationXML/CI/"]
xmlpaths2 = os.path.join(os.path.dirname(__file__), "./data/")


@pytest.mark.parametrize("path", xmlpaths)
def test_XMLStationChannelCatalog(path):
    cat = XMLStationChannelCatalog(path, storage_options={"s3": {"anon": True}})
    empty_inv = cat.get_inventory(DateTimeRange(), Station("non-existent", "non-existent", ""))
    assert len(empty_inv) == 0
    yaq_inv = cat.get_inventory(DateTimeRange(), Station("CI", "YAQ"))
    assert len(yaq_inv) == 1
    assert len(yaq_inv.networks[0].stations) == 1


def test_XMLStationChannelCatalogCustomPath():
    # Check that a custom file name is also found properly, e.g. BK/BK.WINE.xml
    cat = XMLStationChannelCatalog(xmlpaths2, "{network}" + os.path.sep + "{network}.{name}.xml")
    yaq_inv = cat.get_inventory(DateTimeRange(), Station("BK", "WINE"))
    assert len(yaq_inv) == 1
    assert len(yaq_inv.networks[0].stations) == 1


def test_FDSNStationChannelCatalog(tmp_path: str):
    cat = FDSNChannelCatalog("IRIS", tmp_path)
    chan = Channel(ChannelType("BHZ"), Station("UW", "SEP"))
    yaq_inv = cat.get_inventory(DateTimeRange(), chan.station)
    assert len(yaq_inv) == 1
    assert len(yaq_inv.networks[0].stations) == 1
    _ = cat.get_full_channel(DateTimeRange(), chan)

    chan = Channel(ChannelType("ABC"), Station("UW", "DEF"))
    yaq_inv = cat.get_inventory(DateTimeRange(), chan.station)
    assert len(yaq_inv) == 0
