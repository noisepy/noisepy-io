import logging
from abc import ABC, abstractmethod
from functools import lru_cache

import diskcache as dc
import numpy as np
import obspy
import obspy.core.inventory as inventory
import pandas as pd
from datetimerange import DateTimeRange
from obspy import UTCDateTime, read_inventory
from obspy.clients.fdsn import Client

from .datatypes import Channel, Station
from .utils import fs_join, get_filesystem

logger = logging.getLogger(__name__)


class ChannelCatalog(ABC):
    """
    An abstract catalog for getting full channel information (lat, lon, elev, resp)
    """

    def populate_from_inventory(self, inv: obspy.Inventory, ch: Channel) -> Channel:
        filtered = inv.select(network=ch.station.network, station=ch.station.name, channel=ch.type.name)
        if (
            len(filtered) == 0
            or len(filtered.networks[0].stations) == 0
            or len(filtered.networks[0].stations[0].channels) == 0
        ):
            logger.warning(f"Could not find channel {ch} in the inventory")
            return ch

        inv_chan = filtered.networks[0].stations[0].channels[0]
        return Channel(
            ch.type,
            Station(
                network=ch.station.network,
                name=ch.station.name,
                lat=inv_chan.latitude,
                lon=inv_chan.longitude,
                elevation=inv_chan.elevation,
                location=ch.station.location,
            ),
        )

    def get_full_channel(self, timespan: DateTimeRange, channel: Channel) -> Channel:
        inv = self.get_inventory(timespan, channel.station)
        return self.populate_from_inventory(inv, channel)

    @abstractmethod
    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        pass


class XMLStationChannelCatalog(ChannelCatalog):
    """
    A channel catalog that reads <station>.XML files from a directory or an s3://... bucket url path.
    """

    def __init__(self, xmlpath: str, path_format: str = "{network}_{name}.xml", storage_options={}) -> None:
        """
        Constructs a XMLStationChannelCatalog

        Args:
            xmlpath (str): Base directory where to find the files
            path_format (str): Format string to construct the file name from a station.
                               The argument names are 'network' and 'name'.
        """
        super().__init__()
        self.xmlpath = xmlpath
        self.path_format = path_format
        self.fs = get_filesystem(xmlpath, storage_options=storage_options)
        if not self.fs.exists(self.xmlpath):
            raise Exception(f"The XML Station file directory '{xmlpath}' doesn't exist")

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        file_name = self.path_format.format(network=station.network, name=station.name)
        xmlfile = fs_join(self.xmlpath, file_name)
        return self._get_inventory_from_file(xmlfile)

    @lru_cache(maxsize=None)
    def _get_inventory_from_file(self, xmlfile):
        if not self.fs.exists(xmlfile):
            logger.warning(f"Could not find StationXML file {xmlfile}. Returning empty Inventory()")
            return obspy.Inventory()
        with self.fs.open(xmlfile) as f:
            logger.info(f"Reading StationXML file {xmlfile}")
            return read_inventory(f)


class FDSNChannelCatalog(ChannelCatalog):
    """
    A channel catalog that queries the FDSN service
    FDSN ~ International Federation of Digital Seismograph Network
    """

    def __init__(
        self,
        url_key: str,
        cache_dir: str,
    ):
        super().__init__()
        self.url_key = url_key

        logger.info(f"Cache dir: ${cache_dir}")
        self.cache = dc.Cache(cache_dir)

    def get_full_channel(self, timespan: DateTimeRange, channel: Channel) -> Channel:
        inv = self._get_inventory(str(timespan))
        return self.populate_from_inventory(inv, channel)

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        return self._get_inventory(str(timespan))

    def _get_cache_key(self, ts_str: str) -> str:
        return f"{self.url_key}_{ts_str}"

    @lru_cache
    # pass the timestamp (DateTimeRange) as string so that the method is cacheable
    # since DateTimeRange is not hasheable
    def _get_inventory(self, ts_str: str) -> obspy.Inventory:
        ts = DateTimeRange.from_range_text(ts_str)
        key = self._get_cache_key(ts_str)
        inventory = self.cache.get(key, None)
        if inventory is None:
            logging.info(f"Inventory not found in cache for key: '{key}'. Fetching from {self.url_key}.")
            bulk_station_request = [
                ("*", "*", "*", "*", UTCDateTime(ts.start_datetime), UTCDateTime(ts.end_datetime))
            ]
            client = Client(self.url_key)
            inventory = client.get_stations_bulk(bulk_station_request, level="channel")
            self.cache[key] = inventory
        return inventory


class CSVChannelCatalog(ChannelCatalog):
    """
    A channel catalog implentations that reads the station csv file
    """

    def __init__(self, file: str):
        self.df = pd.read_csv(file)

    def get_full_channel(self, timespan: DateTimeRange, ch: Channel) -> Channel:
        ista = self.df[self.df["station"] == ch.station.name].index.values.astype("int64")[0]
        return Channel(
            ch.type,
            Station(
                network=ch.station.network,
                name=ch.station.name,
                lat=self.df.iloc[ista]["latitude"],
                lon=self.df.iloc[ista]["longitude"],
                elevation=self.df.iloc[ista]["elevation"],
                location=ch.station.location,
            ),
        )

    def get_inventory(self, timespan: DateTimeRange, station: Station) -> obspy.Inventory:
        # Build a obspy.Inventory from the dataframe
        network_codes = list(self.df["network"].unique())
        df = self.df
        nets = []
        for net in network_codes:
            sta_names = list(df[df.network == net]["station"].unique())
            stations = []
            for sta in sta_names:
                sta_row = df[df.network == net][df.station == sta].iloc[0]
                lat = sta_row["latitude"]
                lon = sta_row["longitude"]
                elevation = sta_row["elevation"]
                channels = [
                    inventory.Channel(ch, "", lat, lon, elevation, 0)
                    for ch in df[df.network == net][df.station == sta]["channel"].values
                ]
                station = inventory.Station(sta, lat, lon, elevation, channels=channels)
                stations.append(station)
            nets.append(inventory.Network(net, stations))
        return obspy.Inventory(nets)


def sta_info_from_inv(inv: inventory.Inventory):
    """
    this function outputs station info from the obspy inventory object
    (used in S0B)
    PARAMETERS:
    ----------------------
    inv: obspy inventory object
    RETURNS:
    ----------------------
    sta: station name
    net: netowrk name
    lon: longitude of the station
    lat: latitude of the station
    elv: elevation of the station
    location: location code of the station
    """
    # load from station inventory
    sta = inv[0][0].code
    net = inv[0].code
    lon = inv[0][0].longitude
    lat = inv[0][0].latitude
    if inv[0][0].elevation:
        elv = inv[0][0].elevation
    else:
        elv = 0.0

    if inv[0][0][0].location_code:
        location = inv[0][0][0].location_code
    else:
        location = "00"

    return sta, net, lon, lat, elv, location


def stats2inv_mseed(stats, locs: pd.DataFrame) -> inventory.Inventory:
    inv = inventory.Inventory(networks=[], source="homegrown")
    ista = locs[locs["station"] == stats.station].index.values.astype("int64")[0]

    net = inventory.Network(
        # This is the network code according to the SEED standard.
        code=locs.iloc[ista]["network"],
        stations=[],
        description="created from SAC and resp files",
        start_date=stats.starttime,
    )

    sta = inventory.Station(
        # This is the station code according to the SEED standard.
        code=locs.iloc[ista]["station"],
        latitude=locs.iloc[ista]["latitude"],
        longitude=locs.iloc[ista]["longitude"],
        elevation=locs.iloc[ista]["elevation"],
        creation_date=stats.starttime,
        site=inventory.Site(name="First station"),
    )

    cha = inventory.Channel(
        code=stats.channel,
        location_code=stats.location,
        latitude=locs.iloc[ista]["latitude"],
        longitude=locs.iloc[ista]["longitude"],
        elevation=locs.iloc[ista]["elevation"],
        depth=-locs.iloc[ista]["elevation"],
        azimuth=0,
        dip=0,
        sample_rate=stats.sampling_rate,
    )

    response = inventory.response.Response()

    # Now tie it all together.
    cha.response = response
    sta.channels.append(cha)
    net.stations.append(sta)
    inv.networks.append(net)

    return inv


def cc_parameters(cc_para, coor, tcorr, ncorr, comp):
    """
    this function assembles the parameters for the cc function, which is used
    when writing them into ASDF files
    PARAMETERS:
    ---------------------
    cc_para: dict containing parameters used in the fft_cc step
    coor:    dict containing coordinates info of the source and receiver stations
    tcorr:   timestamp matrix
    ncorr:   matrix of number of good segments for each sub-stack/final stack
    comp:    2 character strings for the cross correlation component
    RETURNS:
    ------------------
    parameters: dict containing above info used for later stacking/plotting
    """
    latS = coor["latS"]
    lonS = coor["lonS"]
    latR = coor["latR"]
    lonR = coor["lonR"]
    dt = cc_para["dt"]
    maxlag = cc_para["maxlag"]
    substack = cc_para["substack"]
    cc_method = cc_para["cc_method"]

    dist, azi, baz = obspy.geodetics.base.gps2dist_azimuth(latS, lonS, latR, lonR)
    parameters = {
        "dt": dt,
        "maxlag": int(maxlag),
        "dist": np.float32(dist / 1000),
        "azi": np.float32(azi),
        "baz": np.float32(baz),
        "lonS": np.float32(lonS),
        "latS": np.float32(latS),
        "lonR": np.float32(lonR),
        "latR": np.float32(latR),
        "ngood": ncorr,
        "cc_method": str(cc_method.value),
        "time": tcorr,
        "substack": substack,
        "comp": comp,
    }
    return parameters


# TODO: A channel catalog that uses the files in the SCEDC S3 bucket: s3://scedc-pds/FDSNstationXML/
