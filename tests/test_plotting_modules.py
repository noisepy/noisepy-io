from unittest.mock import MagicMock

import matplotlib
import numpy as np
import pytest

matplotlib.use("Agg")

from noisepy.seis.io.datatypes import Stack, Station
from noisepy.seis.io.plotting_modules import plot_all_moveout, plot_substack_cc

SRC = Station("nw", "sta1")
REC = Station("nw", "sta2")


def _make_stack(dt=0.05, maxlag=10.0, dist=5.0):
    n = int(2 * maxlag / dt) + 1
    return Stack(
        component="ZZ",
        name="Allstack_linear",
        params={"dt": dt, "maxlag": maxlag, "dist": dist, "ngood": 10},
        data=np.random.random(n).astype(np.float32),
    )


# ── plot_substack_cc ──────────────────────────────────────────────────────────


def test_plot_substack_cc_savefig_no_sdir():
    cc_store = MagicMock()
    with pytest.raises(ValueError, match="sdir"):
        plot_substack_cc(cc_store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=True, sdir=None)


def test_plot_substack_cc_no_station_pairs():
    cc_store = MagicMock()
    cc_store.get_station_pairs.return_value = []
    # should return early without raising
    plot_substack_cc(cc_store, ts=MagicMock(), freqmin=0.1, freqmax=1.0, savefig=False)


# ── plot_all_moveout ──────────────────────────────────────────────────────────


def test_plot_all_moveout_savefig_no_sdir():
    stack = _make_stack()
    sta_stacks = [((SRC, REC), [stack])]
    with pytest.raises(ValueError, match="sdir"):
        plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=True, sdir=None)


def test_plot_all_moveout_empty():
    # empty list → early return, no error
    plot_all_moveout([], "Allstack_linear", 0.1, 1.0, "ZZ", 5, savefig=False)


def test_plot_all_moveout_disp_lag_exceeds_maxlag():
    stack = _make_stack(maxlag=10.0)
    sta_stacks = [((SRC, REC), [stack])]
    with pytest.raises(ValueError, match="lag"):
        plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 5, disp_lag=20.0, savefig=False)


def test_plot_all_moveout_no_matching_component():
    stack = _make_stack()
    sta_stacks = [((SRC, REC), [stack])]
    # "ZR" doesn't match "ZZ" in the stack — should log and return, not raise
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZR", 5, savefig=False)


def test_plot_all_moveout_saves_figure(tmp_path):
    stack = _make_stack(dt=0.05, maxlag=5.0, dist=3.0)
    sta_stacks = [((SRC, REC), [stack])]
    sdir = str(tmp_path)
    plot_all_moveout(sta_stacks, "Allstack_linear", 0.1, 1.0, "ZZ", 10, savefig=True, sdir=sdir)
