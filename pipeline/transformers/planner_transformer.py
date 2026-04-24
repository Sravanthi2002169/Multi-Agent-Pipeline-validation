import uuid
from datetime import datetime, timezone

def transform_plan(raw):
    return {
        "schema_version": "1.0",
        "pipeline_run_id": str(uuid.uuid4()),
        "stage": "impact_analysis",   # must match schema
        "stage_status": "SUCCESS",
        "timestamp": datetime.now(timezone.utc).isoformat(),

        "impact_results": [
            {
                "file_path": edit.get("file_path", "unknown"),
                "change_id": edit.get("change_id", "unknown"),
                "severity": "MEDIUM",
                "reason": "Auto-generated from remediation plan",
                "affected_code_block": "",
                "recommended_action": (
                    "REMEDIATE" if edit.get("type") == "rule_engine"
                    else "REVIEW"
                )
            }
            for edit in raw.get("edits", [])
        ],

        "files_analyzed": raw.get("files_total", 0),
        "files_with_impact": raw.get("files_with_edits", 0)
    }