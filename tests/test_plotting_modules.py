from unittest.mock import MagicMock

import matplotlib
import numpy as np
import pytest

matplotlib.use("Agg")

from noisepy.seis.io.datatypes import Stack, Station
from noisepy.seis.io.plotting_modules import plot_all_moveout, plot_substack_cc, plot_waveform

SRC = Station("UW", "STA1")
REC = Station("UW", "STA2")


def _make_stack(dt=0.05, maxlag=10.0, dist=5.0):
    n = int(2 * maxlag / dt) + 1
    return Stack(
        component="ZZ",
        name="Allstack_linear",
        params={"dt": dt, "maxlag": maxlag, "dist": dist, "ngood": 10},
        data=np.random.random(n).astype(np.float32),
    )


def _make_cc(nwin=3, dt=0.05, maxlag=5.0, dist=10.0):
    """Return a mock CrossCorrelation with valid substack data."""
    npts = int(2 * maxlag / dt) + 1
    cc = MagicMock()
    cc.src = "BHZ"
    cc.rec = "BHZ"
    cc.parameters = {
        "substack": True,
        "dt": dt,
        "maxlag": maxlag,
        "dist": dist,
        "ngood": list(range(nwin)),
        "time": [float(1_000_000 + i * 3600) for i in range(nwin)],
    }
    cc.data = np.random.random((nwin, npts)).astype(np.float32)
    return cc


def _make_cc_store(ccs):
    """Return a mock CrossCorrelationDataStore with one station pair."""
    store = MagicMock()
    store.get_station_pairs.return_value = [(SRC, REC)]
    store.read.return_value = ccs
    return store


def _make_raw_store(n_channels=1, npts=400, dt=0.05):
    """Return a (mock RawDataStore, mock timespan) pair."""
    data_arr = np.random.random(npts).astype(np.float32)

    tr_mock = MagicMock()
    tr_mock.stats.delta = dt
    tr_mock.stats.npts = npts
    tr_mock.stats.starttime = "2000-01-01T00:00:00"
    tr_mock.data = data_arr

    channel_data = MagicMock()
    channel_data.stream = [tr_mock]

    comp_names = ["BHZ", "BHN", "BHE"]
    channels = []
    for i in range(n_channels):
        ch = MagicMock()
        ch.station.name = "sta1"
        ch.station.network = "nw"
        ch.type = comp_names[i % 3]
        channels.append(ch)

    ts = MagicMock()
    store = MagicMock()
    store.get_timespans.return_value = [ts]
    store.get_channels.return_value = channels
    store.read_data.return_value = channel_data
    return store, ts


# ── plot_substack_cc ──────────────────────────────────────────────────────────


def test_plot_substack_cc_savefig_no_sdir():
    cc_store = MagicMock()
    with pytest.raises(ValueError, match="sdir"):
        plot_substack_cc(cc_store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=True, sdir=None)


def test_plot_substack_cc_no_station_pairs():
    cc_store = MagicMock()
    cc_store.get_station_pairs.return_value = []
    plot_substack_cc(cc_store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=False)


def test_plot_substack_cc_no_ccs_for_pair():
    store = _make_cc_store([])
    plot_substack_cc(store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=False)


def test_plot_substack_cc_no_substack_flag():
    cc = _make_cc()
    cc.parameters["substack"] = False
    store = _make_cc_store([cc])
    with pytest.raises(ValueError, match="substack"):
        plot_substack_cc(store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=False)


def test_plot_substack_cc_disp_lag_exceeds_maxlag():
    store = _make_cc_store([_make_cc(maxlag=5.0)])
    with pytest.raises(ValueError, match="lag"):
        plot_substack_cc(store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, disp_lag=20.0, savefig=False)


def test_plot_substack_cc_skips_nwin_zero():
    cc = _make_cc(nwin=3)
    cc.data = np.zeros((0, cc.data.shape[1]), dtype=np.float32)
    store = _make_cc_store([cc])
    plot_substack_cc(store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=False)


def test_plot_substack_cc_saves_figure(tmp_path):
    store = _make_cc_store([_make_cc(nwin=3)])
    plot_substack_cc(store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=True, sdir=str(tmp_path))


# ── plot_all_moveout ──────────────────────────────────────────────────────────


def test_plot_all_moveout_savefig_no_sdir():
    stack = _make_stack()
    sta_stacks = [((SRC, REC), [stack])]
    with pytest.raises(ValueError, match="sdir"):
        plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=True, sdir=None)


def test_plot_all_moveout_empty():
    plot_all_moveout([], "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=False)


def test_plot_all_moveout_disp_lag_exceeds_maxlag():
    stack = _make_stack(maxlag=10.0)
    sta_stacks = [((SRC, REC), [stack])]
    with pytest.raises(ValueError, match="lag"):
        plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, disp_lag=20.0, savefig=False)


def test_plot_all_moveout_no_matching_component():
    stack = _make_stack()
    sta_stacks = [((SRC, REC), [stack])]
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZR", 5, savefig=False)


def test_plot_all_moveout_saves_figure(tmp_path):
    stack = _make_stack(dt=0.05, maxlag=5.0, dist=3.0)
    sta_stacks = [((SRC, REC), [stack])]
    sdir = str(tmp_path)
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 10, savefig=True, sdir=sdir)


def test_plot_all_moveout_empty_params():
    stack = Stack(
        component="ZZ",
        name="Allstack_linear",
        params={},
        data=np.array([], dtype=np.float32),
    )
    sta_stacks = [((SRC, REC), [stack])]
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=False)


def test_plot_all_moveout_imshow_path():
    # 20 pairs spread 1 km apart → 19 filled distance bins → ndata.shape[0] >= 10 → imshow branch
    dt, maxlag = 0.05, 2.0
    n = int(2 * maxlag / dt) + 1
    sta_stacks = []
    for i in range(20):
        dist = 0.5 + i  # 0.5, 1.5, ..., 19.5 km
        src = Station("nw", f"s{i:02d}")
        rec = Station("nw", f"r{i:02d}")
        stack = Stack(
            "ZZ",
            "Allstack_linear",
            {"dt": dt, "maxlag": maxlag, "dist": dist, "ngood": 5},
            np.random.random(n).astype(np.float32),
        )
        sta_stacks.append(((src, rec), [stack]))
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 1, savefig=False)


def test_plot_all_moveout_missing_stack_for_one_pair():
    # Second pair has a ZR stack instead of ZZ; load() warns and skips it
    stack_good = _make_stack()
    stack_other_comp = Stack(
        component="ZR",
        name="Allstack_linear",
        params={"dt": 0.05, "maxlag": 10.0, "dist": 8.0, "ngood": 5},
        data=np.random.random(401).astype(np.float32),
    )
    sta_stacks = [
        ((SRC, REC), [stack_good]),
        ((SRC, Station("nw", "sta3")), [stack_other_comp]),
    ]
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=False)


# ── plot_waveform ─────────────────────────────────────────────────────────────


def test_plot_waveform_ts_not_in_store():
    store = MagicMock()
    ts = MagicMock()
    other_ts = MagicMock()  # a different object → ts not in [other_ts]
    store.get_timespans.return_value = [other_ts]
    with pytest.raises(ValueError):
        plot_waveform(store, ts, "nw", "sta1", 0.1, 1.0)


def test_plot_waveform_no_channels():
    store = MagicMock()
    ts = MagicMock()
    store.get_timespans.return_value = [ts]
    store.get_channels.return_value = []
    with pytest.raises(ValueError):
        plot_waveform(store, ts, "nw", "sta1", 0.1, 1.0)


def test_plot_waveform_single_channel():
    store, ts = _make_raw_store(n_channels=1)
    plot_waveform(store, ts, "nw", "sta1", 0.1, 5.0)


def test_plot_waveform_three_channels_savefig(tmp_path):
    store, ts = _make_raw_store(n_channels=3)
    plot_waveform(store, ts, "nw", "sta1", 0.1, 5.0, savefig=True, sdir=str(tmp_path))
