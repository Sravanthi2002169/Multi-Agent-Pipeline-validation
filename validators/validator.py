import json
import os
import certifi
import urllib3
import time
from datetime import datetime
from jsonschema import validate, ValidationError
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

# -------------------------------
# CONFIG
# -------------------------------
MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 2
DEAD_LETTER_CONTAINER = "dead-letter"

# -------------------------------
# SSL Fix
# -------------------------------
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
os.environ['SSL_CERT_FILE'] = certifi.where()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------------
# Blob Service
# -------------------------------
def get_blob_service(connection_string):
    transport = RequestsTransport(connection_verify=False)
    return BlobServiceClient.from_connection_string(
        connection_string,
        transport=transport
    )

# -------------------------------
# Load JSON
# -------------------------------
def load_json(path):
    with open(path, 'r') as file:
        return json.load(file)

# -------------------------------
# Load Business Rules
# -------------------------------
def load_business_rules(path):
    with open(path, 'r') as file:
        return json.load(file)["rules"]

# -------------------------------
# Load from Blob
# -------------------------------
def load_json_from_blob(connection_string, container, blob_path):
    try:
        blob_service_client = get_blob_service(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)

        blob_data = blob_client.download_blob().readall()
        content = blob_data.decode("utf-8")

        try:
            return json.loads(content)
        except:
            return content

    except Exception as e:
        print(f"⚠️ Error reading blob {blob_path}: {e}")
        return None

# -------------------------------
# Get latest blob
# -------------------------------
def get_latest_blob(connection_string, container, prefix):
    blob_service_client = get_blob_service(connection_string)
    container_client = blob_service_client.get_container_client(container)

    blobs = list(container_client.list_blobs(name_starts_with=prefix))

    if not blobs:
        return None

    latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]
    return latest_blob.name

# -------------------------------
# Schema Validation
# -------------------------------
def validate_schema(data, schema):
    try:
        validate(instance=data, schema=schema)
        return []
    except ValidationError as e:
        return [e.message]

# -------------------------------
# Business Rules
# -------------------------------
def validate_business_rules(data, stage, rules):
    errors = []

    for rule in rules:
        if rule.get("stage") != stage:
            continue

        try:
            if rule["type"] == "comparison":
                section = data.get(rule["field"], {})
                left = section.get(rule["left"], 0)
                right = section.get(rule["right"], 0)

                if rule["condition"] == "lte" and left > right:
                    errors.append(rule["description"])

            elif rule["type"] == "array_length":
                for item in data.get(rule["array"], []):
                    if len(item.get(rule["field"], "")) < rule["min_length"]:
                        errors.append(rule["description"])

        except Exception as e:
            errors.append(f"Rule error: {str(e)}")

    return errors

# -------------------------------
# Logging
# -------------------------------
def log_validation(stage, blob_path, result, schema_errors, business_errors):
    log = {
        "log_type": "pipeline_validation",
        "stage": stage,
        "timestamp": datetime.now().isoformat(),
        "output_blob_path": blob_path,
        "validation_result": result,
        "schema_errors": schema_errors,
        "business_rule_errors": business_errors
    }

    print("\n📊 VALIDATION LOG:")
    print(json.dumps(log, indent=2))

# -------------------------------
# CORE VALIDATION
# -------------------------------
def validate_blob(schema_path, connection_string, container, blob_path, stage, rules):

    schema = load_json(schema_path)
    data = load_json_from_blob(connection_string, container, blob_path)

    if data is None:
        return False

    schema_errors = []
    business_errors = []

    if isinstance(data, dict):
        schema_errors = validate_schema(data, schema)

        if schema_errors:
            print(f"❌ Schema Validation Failed: {blob_path}")
        else:
            print(f"✅ Schema Valid: {blob_path}")

    business_errors = validate_business_rules(data, stage, rules)

    if business_errors:
        print("❌ Business Rule Failed")

    result = "PASS" if not (schema_errors or business_errors) else "FAIL"

    log_validation(stage, blob_path, result, schema_errors, business_errors)

    return result == "PASS"

# -------------------------------
# DEAD LETTER
# -------------------------------
def move_to_dead_letter(connection_string, container, blob_path, stage):
    try:
        blob_service_client = get_blob_service(connection_string)

        source_blob = blob_service_client.get_blob_client(container, blob_path)
        dead_blob_path = f"{stage}/{blob_path.split('/')[-1]}"

        dead_blob = blob_service_client.get_blob_client(DEAD_LETTER_CONTAINER, dead_blob_path)

        data = source_blob.download_blob().readall()
        dead_blob.upload_blob(data, overwrite=True)

        print(f"📦 Moved to dead-letter: {dead_blob_path}")

    except Exception as e:
        print(f"⚠️ Dead-letter move failed: {str(e)}")

# -------------------------------
# RETRY LOGIC
# -------------------------------
def validate_with_retry(schema_path, connection_string, container, blob_path, stage, rules):
    attempt = 0

    while attempt <= MAX_RETRIES:
        print(f"\n🔁 Attempt {attempt + 1} for {stage}")

        success = validate_blob(
            schema_path,
            connection_string,
            container,
            blob_path,
            stage,
            rules
        )

        if success:
            print(f"✅ {stage} PASSED")
            return True

        attempt += 1

        if attempt > MAX_RETRIES:
            print("❌ Max retries reached")
            move_to_dead_letter(connection_string, container, blob_path, stage)
            return False

        print(f"⚠️ Retry in {RETRY_DELAY_SECONDS} sec...")
        time.sleep(RETRY_DELAY_SECONDS)

# -------------------------------
# MAIN PIPELINE
# -------------------------------
if __name__ == "__main__":

    CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    CONTAINER = "tenant-qnxtupgd"

    rules = load_business_rules("business_rules.json")

    # Discovery
    if not validate_with_retry(
        "schemas/discovery_schema.json",
        CONNECTION_STRING,
        CONTAINER,
        "agent_handoff/Discovery_Report.json",
        "discovery",
        rules
    ):
        print("🚫 PIPELINE STOPPED at Discovery")
        exit()

    # Impact
    if not validate_with_retry(
        "schemas/impact_schema.json",
        CONNECTION_STRING,
        CONTAINER,
        "agent_handoff/Impact_Report.json",
        "impact",
        rules
    ):
        print("🚫 PIPELINE STOPPED at Impact")
        exit()

    # Remediation
    latest = get_latest_blob(CONNECTION_STRING, CONTAINER, "remediated/")

    if latest:
        print(f"\n🔍 Validating Remediation: {latest}")

        if not validate_with_retry(
            "schemas/remediation_schema.json",
            CONNECTION_STRING,
            CONTAINER,
            latest,
            "remediation",
            rules
        ):
            print("🚫 PIPELINE STOPPED at Remediation")
            exit()