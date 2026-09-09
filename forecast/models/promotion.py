from __future__ import annotations

import json
from pathlib import Path


def promote_manually(candidate_path: Path, production_manifest: Path, *, approved_by: str, validation_summary: dict[str, object]) -> None:
    """Write a production manifest only after an explicit human approval call."""
    if not approved_by.strip():
        raise ValueError("approved_by is required; model promotion is never automatic")
    payload = {
        "candidate": str(candidate_path),
        "approved_by": approved_by,
        "validation_summary": validation_summary,
    }
    production_manifest.parent.mkdir(parents=True, exist_ok=True)
    production_manifest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
