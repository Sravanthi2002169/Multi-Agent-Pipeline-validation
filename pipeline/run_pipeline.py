import os
from validation.validator import validate_with_retry, STAGE_SCHEMA_MAP, get_stage_blob, load_business_rules

def run_pipeline():
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = "tenant-qnxtupgd"
    rules = load_business_rules(os.path.join("validation", "business_rules.json"))

    print("\n Starting Pipeline...\n")

    # -------------------
    # DISCOVERY
    # -------------------
    print(" Running Discovery Agent...")
    # TODO: call your discovery function here

    discovery_blob = get_stage_blob(conn, container, "discovery")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["discovery"],
        conn,
        container,
        discovery_blob,
        "discovery",
        rules
    ):
        exit(" Pipeline stopped at Discovery")

    # -------------------
    # PLANNING
    # -------------------
    print("\n Running Planner Agent...")
    # TODO: call your planner function here

    plan_blob = get_stage_blob(conn, container, "planning")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["planning"],
        conn,
        container,
        plan_blob,
        "planning",
        rules
    ):
        exit(" Pipeline stopped at Planning")

    # -------------------
    # REMEDIATION
    # -------------------
    print("\n Running Remediation Agent...")
    # TODO: call your remediation function here

    remediation_blob = get_stage_blob(conn, container, "remediation")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["remediation"],
        conn,
        container,
        remediation_blob,
        "remediation",
        rules
    ):
        exit(" Pipeline stopped at Remediation")

    print("\n PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_pipeline()
    
print("\n PIPELINE COMPLETED SUCCESSFULLY")
exit(0)