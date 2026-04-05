"""Machine learning utilities (archetype clustering, etc.)."""

from __future__ import annotations

__all__ = [
    "ArchetypeClusteringConfig",
    "ArchetypeClusteringConfigsByRole",
    "ARCHETYPE_CLUSTER_LABELS_BATTER",
    "ARCHETYPE_CLUSTER_LABELS_BY_ROLE",
    "ARCHETYPE_CLUSTER_LABELS_PITCHER",
    "archetype_cluster_label",
    "build_gold_archetype_clustering",
    "build_gold_player_similarity",
    "fit_archetype_clustering",
    "numeric_feature_columns",
    "PlayerSimilarityConfig",
    "prepare_dataframe_for_archetype_clustering",
]

_ARCHETYPE_EXPORTS = frozenset(
    {
        "ArchetypeClusteringConfig",
        "ArchetypeClusteringConfigsByRole",
        "ARCHETYPE_CLUSTER_LABELS_BATTER",
        "ARCHETYPE_CLUSTER_LABELS_BY_ROLE",
        "ARCHETYPE_CLUSTER_LABELS_PITCHER",
        "archetype_cluster_label",
        "build_gold_archetype_clustering",
        "fit_archetype_clustering",
        "numeric_feature_columns",
        "prepare_dataframe_for_archetype_clustering",
    }
)
_SIMILARITY_EXPORTS = frozenset({"build_gold_player_similarity", "PlayerSimilarityConfig"})


def __getattr__(name: str):
    if name in _ARCHETYPE_EXPORTS:
        from . import archetype_clustering as _ac

        return getattr(_ac, name)
    if name in _SIMILARITY_EXPORTS:
        from . import player_similarity as _ps

        return getattr(_ps, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
