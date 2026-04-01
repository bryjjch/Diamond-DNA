"""Machine learning utilities (archetype clustering, etc.)."""

from __future__ import annotations

__all__ = [
    "ArchetypeClusteringConfig",
    "build_gold_archetype_clustering",
    "fit_archetype_clustering",
    "numeric_feature_columns",
]


def __getattr__(name: str):
    if name in __all__:
        from . import archetype_clustering as _ac

        return getattr(_ac, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
