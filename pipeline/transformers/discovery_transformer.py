import uuid
from datetime import datetime, timezone

def transform_discovery(raw):
    return {
        "schema_version": "1.0",
        "pipeline_run_id": str(uuid.uuid4()),
        "release": raw.get("release", "unknown"),
        "stage": "discovery",
        "stage_status": "SUCCESS",
        "timestamp": datetime.now(timezone.utc).isoformat(),

        "summary": {
            "total_changes": raw.get("summary", {}).get("total_changes", 0),
            "total_files_scanned": raw.get("summary", {}).get("scanned", 0),
            "total_files_impacted": raw.get("summary", {}).get("impacted", 0)
        },

        "impacted_files": [
            {
                "file_path": f.get("file_path"),
                "change_ids": f.get("change_ids", []),
                "matched_patterns": [],
                "line_numbers": []
            }
            for f in raw.get("details", [])
        ]
    }