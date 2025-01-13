import os

from noisepy.seis.io.mseedstore import MiniSeedDataStore
from noisepy.seis.io.channelcatalog import XMLStationChannelCatalog
from datetime import datetime, timezone
from datetimerange import DateTimeRange

# timeframe for analysis
start = datetime(2022, 1, 1, tzinfo=timezone.utc)
end = datetime(2022, 1, 4, tzinfo=timezone.utc)
timerange = DateTimeRange(start, end)

DATA = os.path.join(os.path.dirname(__file__), "./data/mSEED")
STATION_XML = os.path.join(os.path.dirname(__file__), "./data/CI/")
catalog = XMLStationChannelCatalog(STATION_XML, path_format='{network}.{name}.xml')
store = MiniSeedDataStore(DATA, catalog, date_range=timerange)

def test_mseedstore():
    span = store.get_timespans()
    assert len(span) == 3

    channels = store.get_channels(span[0])
    assert len(channels) == 1
    assert len(store.read_data(span[0], channels[0]).stream) == 1