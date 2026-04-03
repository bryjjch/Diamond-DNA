import numpy as np
import pandas as pd

from src.ml.archetype_finetune import (
    grid_sweep_pca_and_gmm,
    grid_sweep_pca_and_k,
    scaled_feature_matrix,
)


def test_grid_sweep_pca_and_k_runs():
    rng = np.random.default_rng(0)
    n = 80
    df = pd.DataFrame(
        {
            "player_id": np.arange(n),
            "year": [2024] * n,
            "role": ["batter"] * n,
            "n_pitches_total": [500] * n,
            "a": rng.normal(size=n),
            "b": rng.normal(size=n),
            "c": rng.normal(size=n),
        }
    )
    X_scaled, _, _ = scaled_feature_matrix(df)
    out = grid_sweep_pca_and_k(
        X_scaled,
        pca_n_components_list=[2, 3],
        k_min=2,
        k_max=5,
        random_state=0,
        n_init=5,
    )
    assert not out.empty
    assert set(out.columns) >= {
        "pca_n_components",
        "k",
        "inertia",
        "silhouette",
        "davies_bouldin",
        "pca_total_explained_variance",
    }


def test_grid_sweep_pca_and_gmm_runs():
    rng = np.random.default_rng(0)
    n = 80
    df = pd.DataFrame(
        {
            "player_id": np.arange(n),
            "year": [2024] * n,
            "role": ["batter"] * n,
            "n_pitches_total": [500] * n,
            "a": rng.normal(size=n),
            "b": rng.normal(size=n),
            "c": rng.normal(size=n),
        }
    )
    X_scaled, _, _ = scaled_feature_matrix(df)
    out = grid_sweep_pca_and_gmm(
        X_scaled,
        pca_n_components_list=[2, 3],
        k_min=2,
        k_max=5,
        random_state=0,
        n_init=5,
        covariance_type="full",
    )
    assert not out.empty
    assert set(out.columns) >= {
        "pca_n_components",
        "k",
        "gmm_aic",
        "gmm_bic",
        "gmm_lower_bound",
        "silhouette",
        "davies_bouldin",
        "pca_total_explained_variance",
        "gmm_covariance_type",
    }
