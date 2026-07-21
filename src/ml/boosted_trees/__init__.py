"""Gradient-boosted tree projections: the first learned model over the gold matrix.

Currently the XGBoost walk-forward projection (``xgboost_projection``): one
regressor per (role, target stat), trained on all seasons before the target
year and scored with the same metrics as the Marcel baseline.
"""
