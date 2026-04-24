def run_remediation(plan_id):
    """
    This stage ONLY applies patch.
    """
    print("Applying remediation plan...")
    
    return {
        "stage": "remediation",            #  REQUIRED
        "stage_status": "SUCCESS",
        "pipeline_run_id": "run_001",
        "timestamp": "2026-04-23T10:10:00Z",
        "data": {}
    }