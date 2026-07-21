"""Machine learning utilities (archetype clustering, etc.)."""

from __future__ import annotations

__all__ = [
    "ArchetypeClusteringConfig",
    "ArchetypeClusteringConfigsByRole",
    "build_cluster_labels",
    "build_gold_archetype_clustering",
    "build_gold_player_similarity",
    "build_marcel_baseline",
    "build_xgboost_projections",
    "compare_model_metrics",
    "compute_cluster_profiles",
    "fit_archetype_clustering",
    "generate_cluster_labels",
    "MarcelConfig",
    "numeric_feature_columns",
    "PlayerSimilarityConfig",
    "prepare_dataframe_for_archetype_clustering",
    "summarize_comparison",
    "XGBoostProjectionConfig",
]

_ARCHETYPE_EXPORTS = frozenset(
    {
        "ArchetypeClusteringConfig",
        "ArchetypeClusteringConfigsByRole",
        "build_gold_archetype_clustering",
        "fit_archetype_clustering",
        "numeric_feature_columns",
        "prepare_dataframe_for_archetype_clustering",
    }
)
_LABELING_EXPORTS = frozenset(
    {
        "build_cluster_labels",
        "compute_cluster_profiles",
        "generate_cluster_labels",
    }
)
_SIMILARITY_EXPORTS = frozenset({"build_gold_player_similarity", "PlayerSimilarityConfig"})
_BASELINE_EXPORTS = frozenset({"build_marcel_baseline", "MarcelConfig"})
_XGBOOST_EXPORTS = frozenset({"build_xgboost_projections", "XGBoostProjectionConfig"})
_EVALUATION_EXPORTS = frozenset({"compare_model_metrics", "summarize_comparison"})


def __getattr__(name: str):
    if name in _ARCHETYPE_EXPORTS:
        from .archetypes import archetype_clustering as _ac

        return getattr(_ac, name)
    if name in _LABELING_EXPORTS:
        from .archetypes import archetype_labeling as _al

        return getattr(_al, name)
    if name in _SIMILARITY_EXPORTS:
        from .knn_neighbours import player_similarity as _ps

        return getattr(_ps, name)
    if name in _BASELINE_EXPORTS:
        from .baselines import marcel_baseline as _mb

        return getattr(_mb, name)
    if name in _XGBOOST_EXPORTS:
        from .boosted_trees import xgboost_projection as _xp

        return getattr(_xp, name)
    if name in _EVALUATION_EXPORTS:
        from .evaluation import compare_projections as _cp

        return getattr(_cp, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
