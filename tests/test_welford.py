import numpy as np

import welford

import ecofuture_preproc.summary_stats


def test_welford():
    seed = 51321412

    rand = np.random.default_rng(seed=seed)

    data = rand.random((10, 3, 4, 4))

    w_tracker = welford.Welford()
    tracker = ecofuture_preproc.summary_stats.StatTracker()

    for data_sample in data:
        data_sample = data_sample.flatten()

        w_tracker.add_all(data_sample)
        tracker.update(data_sample)

    assert np.isclose(w_tracker.mean, tracker.mean)
    assert np.isclose(float(np.sqrt(w_tracker.var_p)), tracker.sd)
