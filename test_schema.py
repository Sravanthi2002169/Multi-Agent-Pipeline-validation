import json
from jsonschema import validate, ValidationError

# Load schema
with open("schemas/impact_schema.json") as f:
    schema = json.load(f)

# Sample valid data (test case)
valid_data = {
    "schema_version": "1.0",
    "pipeline_run_id": "123",
    "stage": "impact_analysis",
    "stage_status": "SUCCESS",
    "impact_results": [
        {
            "file_path": "file.sql",
            "change_id": "chg1",
            "severity": "HIGH",
            "reason": "This is a valid reason with more than 20 chars",
            "affected_code_block": "SELECT * FROM table",
            "recommended_action": "REMEDIATE"
        }
    ],
    "files_analyzed": 1,
    "files_with_impact": 1
}

# Validate
try:
    validate(instance=valid_data, schema=schema)
    print(" VALID DATA")
except ValidationError as e:
    print(" INVALID DATA")
    print(e)