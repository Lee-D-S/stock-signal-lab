from __future__ import annotations

from collections.abc import Iterator

import pandas as pd


def walk_forward_splits(frame: pd.DataFrame, *, date_column: str = "date", n_splits: int = 3, test_size: int = 20, purge: int = 20, min_train_dates: int = 60) -> Iterator[tuple[pd.Index, pd.Index]]:
    """Yield date-based folds with a purge gap before every test fold."""
    dates = pd.Series(pd.to_datetime(frame[date_column], errors="raise").dt.normalize().unique()).sort_values().reset_index(drop=True)
    if len(dates) < min_train_dates + purge + test_size:
        return
    first_test_start = len(dates) - n_splits * test_size
    for fold in range(n_splits):
        test_start = first_test_start + fold * test_size
        test_end = test_start + test_size
        train_end = test_start - purge
        if train_end < min_train_dates:
            continue
        train_dates = set(dates.iloc[:train_end])
        test_dates = set(dates.iloc[test_start:test_end])
        train_index = frame.index[pd.to_datetime(frame[date_column]).dt.normalize().isin(train_dates)]
        test_index = frame.index[pd.to_datetime(frame[date_column]).dt.normalize().isin(test_dates)]
        if len(train_index) and len(test_index):
            yield train_index, test_index
