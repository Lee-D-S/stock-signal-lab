from __future__ import annotations

import json
import pickle
from pathlib import Path

from .sklearn_models import FittedModel


def save_model(model: FittedModel, path: Path, *, model_version: str, snapshot_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        pickle.dump(model, handle, protocol=pickle.HIGHEST_PROTOCOL)
    path.with_suffix(path.suffix + ".json").write_text(json.dumps({
        "model_version": model_version,
        "snapshot_id": snapshot_id,
        "name": model.name,
        "task": model.task,
        "feature_columns": list(model.feature_columns),
        "rows": model.rows,
    }, indent=2), encoding="utf-8")


def load_model(path: Path) -> FittedModel:
    with path.open("rb") as handle:
        model = pickle.load(handle)
    if not isinstance(model, FittedModel):
        raise TypeError(f"Unsupported model bundle: {path}")
    return model
