from __future__ import annotations

import pandas as pd

from .filters import UniverseFilterConfig, apply_universe_filters


def build_point_in_time_universe(security_master: pd.DataFrame, *, as_of: str | pd.Timestamp, config: UniverseFilterConfig | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    return apply_universe_filters(security_master, as_of=pd.Timestamp(as_of).normalize(), config=config)
