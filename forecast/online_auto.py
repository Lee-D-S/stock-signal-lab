from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path

from forecast.data.corp_codes import fetch_dart_corp_code_map
from forecast.data.storage import default_artifact_root, write_parquet
from forecast.online import collect_online
from forecast.universe.kis import fetch_market_universe


def run_auto_collection(*, as_of: str | None = None, artifact_root: Path | None = None) -> dict[str, object]:
    root = artifact_root or default_artifact_root()
    universe = asyncio.run(fetch_market_universe())
    corp_codes = asyncio.run(fetch_dart_corp_code_map())
    if not universe.empty:
        universe["corp_code"] = universe["ticker"].map(corp_codes)
    as_of_date = as_of or date.today().isoformat()
    universe_path = root / f"universe_{as_of_date}.parquet"
    write_parquet(universe, universe_path, artifact_type="point_in_time_universe", as_of=as_of_date, code_version="forecast-0.1.0")
    result = collect_online(universe_path, as_of=as_of, artifact_root=root)
    result["universe_path"] = str(universe_path)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a KIS/DART point-in-time universe and collect numeric data")
    parser.add_argument("--as-of")
    parser.add_argument("--artifact-root", type=Path)
    args = parser.parse_args()
    print(run_auto_collection(as_of=args.as_of, artifact_root=args.artifact_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
