import uuid
from datetime import datetime, timezone

def transform_remediation(raw):
    return {
        "schema_version": "1.0",
        "pipeline_run_id": str(uuid.uuid4()),
        "stage": "remediation",
        "stage_status": "SUCCESS",
        "timestamp": datetime.now(timezone.utc).isoformat(),

        "remediated_files": [
            {
                "original_path": f.get("source_file", "unknown"),
                "remediated_path": f.get("target_file", "unknown"),
                "rollback_path": f.get("backup_file", "unknown"),
                "changes_applied": f.get("changes", []),
                "change_id": f.get("change_id", "unknown")
            }
            for f in raw.get("files", [])
        ],

        "files_remediated": raw.get("files_modified", 0),
        "files_skipped": raw.get("files_unchanged", 0),

        "skip_reasons": [
            {
                "file_path": f.get("source_file", "unknown"),
                "reason": "No changes applied"
            }
            for f in raw.get("files", [])
            if not f.get("changes")
        ]
    }