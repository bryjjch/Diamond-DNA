"""
Offline exploration for PCA dimensionality and cluster count (KMeans and GMM sweeps).

Production ``build_gold_archetype_clustering`` uses GaussianMixture; KMeans helpers remain
for notebooks that document the abandoned KMeans path.

Uses the same frame preparation and feature column rules as production clustering.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from .archetype_clustering import (
    numeric_feature_columns,
    prepare_dataframe_for_archetype_clustering,
)


def scaled_feature_matrix(df: pd.DataFrame) -> Tuple[np.ndarray, List[str], StandardScaler]:
    """Same indexing and feature columns as ``fit_archetype_clustering``."""
    df_i = prepare_dataframe_for_archetype_clustering(df)
    cols = numeric_feature_columns(df_i)
    if not cols:
        raise ValueError("No numeric feature columns.")
    X = df_i[cols].to_numpy(dtype=np.float64, copy=True)
    if np.isnan(X).any():
        raise ValueError("NaN in feature matrix.")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, cols, scaler


def pca_cumulative_variance(
    X_scaled: np.ndarray,
    *,
    max_components: int,
    random_state: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Return (cumulative explained variance, per-component ratios) for up to ``max_components`` PCs."""
    n_samples, n_features = X_scaled.shape
    n_comp = min(max_components, n_features, max(1, n_samples - 1))
    pca = PCA(n_components=n_comp, random_state=random_state)
    pca.fit(X_scaled)
    ratios = pca.explained_variance_ratio_
    cum = np.cumsum(ratios)
    return cum, ratios


def k_sweep_for_pca_space(
    X_pca: np.ndarray,
    *,
    k_min: int,
    k_max: int,
    random_state: int,
    n_init: int,
) -> List[Dict[str, Any]]:
    """Inertia / silhouette / Davies–Bouldin for each k on fixed PCA coordinates."""
    n_samples = X_pca.shape[0]
    k_max = min(k_max, n_samples - 1)
    k_max = max(k_max, k_min)
    curve: List[Dict[str, Any]] = []
    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=random_state, n_init=n_init)
        labels = km.fit_predict(X_pca)
        sil = float("nan")
        if k >= 2 and n_samples > k:
            try:
                sil = float(silhouette_score(X_pca, labels))
            except ValueError:
                pass
        db = float("nan")
        try:
            db = float(davies_bouldin_score(X_pca, labels))
        except ValueError:
            pass
        curve.append(
            {
                "k": k,
                "inertia": float(km.inertia_),
                "silhouette": sil,
                "davies_bouldin": db,
            }
        )
    return curve


def grid_sweep_pca_and_k(
    X_scaled: np.ndarray,
    *,
    pca_n_components_list: List[int],
    k_min: int,
    k_max: int,
    random_state: int,
    n_init: int,
) -> pd.DataFrame:
    """
    For each candidate PCA size, fit PCA → transform → sweep k.

    Returns a long table with columns including ``pca_n_components``, ``k``, metrics.
    """
    rows: List[Dict[str, Any]] = []
    n_samples, n_features = X_scaled.shape
    for npc in pca_n_components_list:
        max_rank = min(n_features, max(1, n_samples - 1))
        n_use = min(int(npc), max_rank)
        if n_use < 1:
            continue
        pca = PCA(n_components=n_use, random_state=random_state)
        X_pca = pca.fit_transform(X_scaled)
        total_var = float(np.sum(pca.explained_variance_ratio_))
        for m in k_sweep_for_pca_space(
            X_pca, k_min=k_min, k_max=k_max, random_state=random_state, n_init=n_init
        ):
            rows.append(
                {
                    "pca_n_components": n_use,
                    "pca_total_explained_variance": total_var,
                    **m,
                }
            )
    return pd.DataFrame(rows)


def gmm_sweep_for_pca_space(
    X_pca: np.ndarray,
    *,
    k_min: int,
    k_max: int,
    random_state: int,
    n_init: int,
    covariance_type: str = "full",
) -> List[Dict[str, Any]]:
    """AIC / BIC / silhouette / Davies–Bouldin for each n_components on fixed PCA coordinates."""
    n_samples = X_pca.shape[0]
    k_max = min(k_max, n_samples - 1)
    k_max = max(k_max, k_min)
    curve: List[Dict[str, Any]] = []
    for k in range(k_min, k_max + 1):
        gmm = GaussianMixture(
            n_components=k,
            covariance_type=covariance_type,
            random_state=random_state,
            n_init=n_init,
        )
        gmm.fit(X_pca)
        labels = gmm.predict(X_pca)
        sil = float("nan")
        if k >= 2 and n_samples > k:
            try:
                sil = float(silhouette_score(X_pca, labels))
            except ValueError:
                pass
        db = float("nan")
        try:
            db = float(davies_bouldin_score(X_pca, labels))
        except ValueError:
            pass
        curve.append(
            {
                "k": k,
                "gmm_aic": float(gmm.aic(X_pca)),
                "gmm_bic": float(gmm.bic(X_pca)),
                "gmm_lower_bound": float(gmm.lower_bound_),
                "silhouette": sil,
                "davies_bouldin": db,
            }
        )
    return curve


def grid_sweep_pca_and_gmm(
    X_scaled: np.ndarray,
    *,
    pca_n_components_list: List[int],
    k_min: int,
    k_max: int,
    random_state: int,
    n_init: int,
    covariance_type: str = "full",
) -> pd.DataFrame:
    """
    For each candidate PCA size, fit PCA → transform → GMM sweep over n_components.

    Returns a long table with columns including ``pca_n_components``, ``k`` (n_components), metrics.
    """
    rows: List[Dict[str, Any]] = []
    n_samples, n_features = X_scaled.shape
    for npc in pca_n_components_list:
        max_rank = min(n_features, max(1, n_samples - 1))
        n_use = min(int(npc), max_rank)
        if n_use < 1:
            continue
        pca = PCA(n_components=n_use, random_state=random_state)
        X_pca = pca.fit_transform(X_scaled)
        total_var = float(np.sum(pca.explained_variance_ratio_))
        for m in gmm_sweep_for_pca_space(
            X_pca,
            k_min=k_min,
            k_max=k_max,
            random_state=random_state,
            n_init=n_init,
            covariance_type=covariance_type,
        ):
            rows.append(
                {
                    "pca_n_components": n_use,
                    "pca_total_explained_variance": total_var,
                    "gmm_covariance_type": covariance_type,
                    **m,
                }
            )
    return pd.DataFrame(rows)
