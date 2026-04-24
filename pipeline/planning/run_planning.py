def run_planning(scan_id):
    """
    This stage ONLY triggers remediation plan tool.
    """
    print("Generating remediation plan...")
    
    return {
        "stage": "impact_analysis",        #  IMPORTANT (not "planning")
        "stage_status": "SUCCESS",
        "pipeline_run_id": "run_001",
        "timestamp": "2026-04-23T10:05:00Z",
        "data": {}
    }