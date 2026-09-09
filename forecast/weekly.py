from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from forecast.data.storage import default_artifact_root, read_frame, snapshot_id, write_markdown
from forecast.features.builder import build_numeric_features, feature_columns
from forecast.labels.returns import add_return_labels
from forecast.models.registry import save_model
from forecast.models.sklearn_models import fit_model
from forecast.pipelines.weekly_train import train_and_evaluate


def run_weekly(input_path: Path, *, artifact_root: Path | None = None) -> dict[str, object]:
    frame = read_frame(input_path)
    if not {"future_return_1d", "direction_1d"}.issubset(frame.columns):
        frame = add_return_labels(build_numeric_features(frame))
    root = artifact_root or default_artifact_root()
    as_of = pd.to_datetime(frame["date"]).max().date().isoformat()
    snapshot = snapshot_id(as_of, tuple(frame.columns), len(frame))
    columns = feature_columns(frame)
    evaluation: dict[str, object] = {}
    for horizon in (1, 5, 20):
        evaluation[str(horizon)] = train_and_evaluate(frame, horizon=horizon, model_name="logistic", n_splits=3, test_size=20, purge=horizon)
        trainable = frame.dropna(subset=columns + [f"direction_{horizon}d", f"future_return_{horizon}d"])
        classifier = fit_model(trainable, feature_columns=columns, target_column=f"direction_{horizon}d", task="classification", model_name="logistic")
        regressor = fit_model(trainable, feature_columns=columns, target_column=f"future_return_{horizon}d", task="regression", model_name="ridge")
        version = f"candidate-{horizon}d-{snapshot}"
        save_model(classifier, root / f"{version}-classifier.pkl", model_version=version, snapshot_id=snapshot)
        save_model(regressor, root / f"{version}-regressor.pkl", model_version=version, snapshot_id=snapshot)
    report = root / f"weekly_evaluation_{as_of}.md"
    write_markdown(report, "Forecast weekly candidate evaluation", {
        "Metadata": f"- as_of: `{as_of}`\n- snapshot_id: `{snapshot}`\n- status: `candidate-not-promoted`",
        "Metrics": f"```json\n{json.dumps(evaluation, ensure_ascii=False, indent=2)}\n```",
        "Promotion": "Candidate bundles require explicit human validation and a manual production manifest update.",
    })
    return {"as_of": as_of, "snapshot_id": snapshot, "report": str(report), "status": "candidate-not-promoted"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run weekly leakage-safe candidate evaluation")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path)
    args = parser.parse_args()
    print(run_weekly(args.input, artifact_root=args.artifact_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
