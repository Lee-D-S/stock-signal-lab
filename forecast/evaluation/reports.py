from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from forecast.data.storage import write_markdown


def write_evaluation_report(path: Path, *, as_of: str, snapshot_id: str, metrics: dict[str, Any], model_status: str = "candidate") -> None:
    write_markdown(path, "Forecast evaluation report", {
        "Metadata": f"- as_of: `{as_of}`\n- snapshot_id: `{snapshot_id}`\n- model_status: `{model_status}`",
        "Metrics": f"```json\n{json.dumps(metrics, ensure_ascii=False, indent=2)}\n```",
    })
