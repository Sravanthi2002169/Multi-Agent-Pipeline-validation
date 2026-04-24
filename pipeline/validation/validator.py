import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import json
import os
import time
import certifi
import urllib3
from jsonschema import validate, ValidationError
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport
from datetime import datetime, timezone
datetime.now(timezone.utc)

# -------------------------------
# CONFIG
# -------------------------------
MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 2
DEAD_LETTER_CONTAINER = "dead-letter"
LOG_CONTAINER = "pipeline-logs"

# Stage → Blob prefix mapping (CRITICAL FIX)
STAGE_BLOB_PREFIX = {
    "discovery": "agent_handoff/sp_discovery_result",
    "planning": "agent_handoff/remediation_plan",
    "remediation": "agent_handoff/apply_result"
}

# Stage → Schema mapping
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STAGE_SCHEMA_MAP = {
    "discovery": os.path.join(BASE_DIR, "schemas", "discovery_schema.json"),
    "planning": os.path.join(BASE_DIR, "schemas", "impact_schema.json"),
    "remediation": os.path.join(BASE_DIR, "schemas", "remediation_schema.json")
}

# -------------------------------
# SSL FIX
# -------------------------------
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------------
# BLOB SERVICE
# -------------------------------
def get_blob_service(connection_string):
    return BlobServiceClient.from_connection_string(
        connection_string,
        transport=RequestsTransport(connection_verify=False)
    )

# -------------------------------
# LOADERS
# -------------------------------
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def load_business_rules(path):
    return load_json(path).get("rules", [])

def load_json_from_blob(connection_string, container, blob_path):
    try:
        client = get_blob_service(connection_string)
        blob = client.get_blob_client(container, blob_path)

        content = blob.download_blob().readall().decode("utf-8")

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content

    except Exception as e:
        print(f"⚠️ Blob read failed [{blob_path}]: {e}")
        return None

# -------------------------------
# GET LATEST BLOB
# -------------------------------
def get_latest_blob(connection_string, container, prefix):
    client = get_blob_service(connection_string)
    blobs = list(client.get_container_client(container).list_blobs(name_starts_with=prefix))

    if not blobs:
        return None

    return sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0].name

def get_stage_blob(connection_string, container, stage):
    prefix = STAGE_BLOB_PREFIX[stage]
    return get_latest_blob(connection_string, container, prefix)

# -------------------------------
# VALIDATIONS
# -------------------------------
def validate_schema(data, schema):
    try:
        validate(instance=data, schema=schema)
        return []
    except ValidationError as e:
        return [e.message]

def validate_business_rules(data, stage, rules):
    errors = []

    for rule in rules:
        if rule.get("stage") != stage:
            continue

        try:
            if rule["type"] == "comparison":
                section = data.get(rule["field"], {}) if rule.get("field") else data
                left = section.get(rule["left"], 0)
                right = section.get(rule["right"], 0)

            if rule["condition"] == "lte" and left > right:
                    errors.append(rule["description"])

            elif rule["type"] == "array_length":
                for item in data.get(rule["array"], []):
                    if len(item.get(rule["field"], "")) < rule["min_length"]:
                        errors.append(rule["description"])

            elif rule["type"] == "string_check":
                if isinstance(data, str) and len(data.strip()) < rule["min_length"]:
                    errors.append(rule["description"])

            elif rule["type"] == "custom":
                if rule["id"] == "IMPACT_008":
                    impact = data.get("impact_results", [])
                    expected = data.get("files_with_impact")

                    if expected is not None and len(impact) != expected:
                        errors.append(rule["description"])

        except Exception as e:
            errors.append(f"{rule.get('id')} error: {str(e)}")

    return errors

# -------------------------------
# LOGGING
# -------------------------------
def log_validation(stage, blob_path, result, schema_errors, business_errors, data):
    try:
        run_id = data.get("pipeline_run_id", "unknown")

        log = {
            "log_type": "pipeline_validation",
            "pipeline_run_id": run_id,
            "stage": stage,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "blob_path": blob_path,
            "result": result,
            "schema_errors": schema_errors,
            "business_errors": business_errors,
            "schema_version": data.get("schema_version", "unknown")
        }

        print("\n📊 VALIDATION LOG")
        print(json.dumps(log, indent=2))

        # Save locally
        os.makedirs("logs", exist_ok=True)
        with open(f"logs/{stage}_{run_id}.json", "w") as f:
            json.dump(log, f, indent=2)

        # Upload to blob
        upload_log(log, stage, run_id)

    except Exception as e:
        print(f"⚠️ Logging failed: {e}")

def upload_log(log, stage, run_id):
    try:
        conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        client = get_blob_service(conn)

        blob_name = f"{stage}/{run_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        client.get_blob_client(LOG_CONTAINER, blob_name).upload_blob(
            json.dumps(log),
            overwrite=True
        )

        print(f"☁️ Uploaded log → {blob_name}")

    except Exception as e:
        print(f"⚠️ Blob log upload failed: {e}")

# -------------------------------
# DEAD LETTER
# -------------------------------
def move_to_dead_letter(connection_string, container, blob_path, stage):
    try:
        client = get_blob_service(connection_string)

        src = client.get_blob_client(container, blob_path)
        dest_path = f"{stage}/{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{os.path.basename(blob_path)}"

        data = src.download_blob().readall()
        client.get_blob_client(DEAD_LETTER_CONTAINER, dest_path).upload_blob(data, overwrite=True)

        print(f"📦 Dead-lettered → {dest_path}")

    except Exception as e:
        print(f"⚠️ Dead-letter failed: {e}")

# -------------------------------
# CORE VALIDATION
# -------------------------------
def validate_blob(schema_path, connection_string, container, blob_path, stage, rules):
    schema = load_json(schema_path)
    raw = load_json_from_blob(connection_string, container, blob_path)
    
    if raw is None:
        return False
    
    if stage == "discovery":
        from pipeline.transformers.discovery_transformer import transform_discovery
        data = transform_discovery(raw)
    elif stage == "planning":
        from pipeline.transformers.planner_transformer import transform_plan
        data = transform_plan(raw)
    elif stage == "remediation":
        from pipeline.transformers.remediation_transformer import transform_remediation
        data = transform_remediation(raw)
    else:
        data = raw

    if not isinstance(data, dict):  # SAFETY FIX
        print("⚠️ Invalid data format (not JSON object)")
        return False

    schema_errors = validate_schema(data, schema)
    business_errors = validate_business_rules(data, stage, rules)

    result = "PASS" if not (schema_errors or business_errors) else "FAIL"

    log_validation(stage, blob_path, result, schema_errors, business_errors, data)

    return result == "PASS"

# -------------------------------
# RETRY WRAPPER
# -------------------------------
def validate_with_retry(schema_path, connection_string, container, blob_path, stage, rules):
    for attempt in range(MAX_RETRIES + 1):
        print(f"\n🔁 Attempt {attempt + 1} → {stage}")

        if validate_blob(schema_path, connection_string, container, blob_path, stage, rules):
            print(f"✅ {stage} PASSED")
            return True

        if attempt == MAX_RETRIES:
            print("❌ Max retries reached")
            move_to_dead_letter(connection_string, container, blob_path, stage)
            return False

        time.sleep(RETRY_DELAY_SECONDS)

# -------------------------------
# MAIN PIPELINE VALIDATION
# -------------------------------
if __name__ == "__main__":
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = "tenant-qnxtupgd"
    rules = load_business_rules("business_rules.json")

    # -------------------------
    # DISCOVERY
    # -------------------------
    discovery_blob = get_stage_blob(conn, container, "discovery")

    if not discovery_blob:
        exit("❌ No discovery output found")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["discovery"],
        conn,
        container,
        discovery_blob,
        "discovery",
        rules
    ):
        exit("🚫 Stopped at Discovery")
        
        

    # -------------------------
    # PLANNING
    # -------------------------
    impact_blob = get_stage_blob(conn, container, "planning")

    if not impact_blob:
        exit("❌ No impact output found")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["planning"],
        conn,
        container,
        impact_blob,
        "planning",
        rules
    ):
        exit("🚫 Stopped at Planning")

    # -------------------------
    # REMEDIATION
    # -------------------------
    remediation_blob = get_stage_blob(conn, container, "remediation")

    if not remediation_blob:
        exit("❌ No remediation output found")

    if not validate_with_retry(
        STAGE_SCHEMA_MAP["remediation"],
        conn,
        container,
        remediation_blob,
        "remediation",
        rules
    ):
        exit("🚫 Stopped at Remediation")

    print("\n✅ FULL PIPELINE VALIDATION PASSED")