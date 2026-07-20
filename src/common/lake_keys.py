"""S3 key builders for the data lake.

Centralizes the layout so producers and consumers cannot drift apart:

- ``gold_prefix``        feature tables written by ``data_pipeline.gold``
- ``predictions_prefix`` model inference outputs the API serves
- ``models_prefix``      fitted estimators + evaluation metadata (not served)
"""

from __future__ import annotations


def gold_feature_key(gold_prefix: str, role: str, year: int, filename: str) -> str:
    """Key for a gold-layer feature table partition."""
    return f"{gold_prefix.strip('/')}/{role}/year={year}/{filename}"


def prediction_key(
    predictions_prefix: str, artifact: str, role: str, year: int, filename: str
) -> str:
    """Key for a model output the API reads, e.g. archetypes/neighbors/marcel."""
    return f"{predictions_prefix.strip('/')}/{artifact}/{role}/year={year}/{filename}"


def model_key(models_prefix: str, model: str, role: str, year: int, filename: str) -> str:
    """Key for a fitted estimator or its training/evaluation metadata."""
    return f"{models_prefix.strip('/')}/{model}/{role}/year={year}/{filename}"
