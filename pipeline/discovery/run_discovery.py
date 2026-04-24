def run_discovery(manifest_path, sp_folder_prefix):
    """
    This stage ONLY triggers discovery tool.
    No logic here.
    """
    print("Triggering SP Discovery...")
    
    # Placeholder for tool call
    return {
        "stage": "discovery",              # ✅ REQUIRED
        "stage_status": "SUCCESS",         # ✅ REQUIRED (enum)
        "pipeline_run_id": "run_001",      # ✅ REQUIRED (temporary for now)
        "timestamp": "2026-04-23T10:00:00Z",  # ✅ REQUIRED
        "data": {}                         # placeholder
    }