from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from . import core


def available_candidates(frame: pd.DataFrame) -> tuple[core.CandidateSpec, ...]:
    return tuple(
        spec
        for spec in core.DEFAULT_CANDIDATES
        if core.feature_group_columns(frame, spec.feature_group)
    )


def run_historical_replay(
    raw: pd.DataFrame,
    *,
    config: core.ExperimentConfig | None = None,
    artifact_root: Path | None = None,
    complete_years: Iterable[int] = range(2015, 2026),
) -> dict[str, object]:
    """Run replay while skipping candidate groups absent from the supplied snapshot."""
    config = config or core.ExperimentConfig()
    features = core.build_feature_frame(raw)
    candidates = available_candidates(features)
    if not candidates:
        raise ValueError("No candidate feature groups are available in the supplied snapshot")

    original = core.DEFAULT_CANDIDATES
    try:
        core.DEFAULT_CANDIDATES = candidates
        return core.run_historical_replay(
            raw,
            config=config,
            artifact_root=artifact_root,
            complete_years=complete_years,
        )
    finally:
        core.DEFAULT_CANDIDATES = original

