import json
from jsonschema import validate, ValidationError
from azure.storage.blob import BlobServiceClient


# -------------------------------
# Load local schema JSON
# -------------------------------
def load_json(path):
    with open(path, 'r') as file:
        return json.load(file)


# -------------------------------
# Load JSON from Azure Blob
# -------------------------------
def load_json_from_blob(connection_string, container, blob_path):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)

        blob_data = blob_client.download_blob().readall()
        return blob_data.decode("utf-8")

    except Exception as e:
        print(f"⚠️ Error reading blob {blob_path}: {e}")
        return None


# -------------------------------
# Get latest remediation file
# -------------------------------
def get_latest_blob(connection_string, container, prefix):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)

    blobs = list(container_client.list_blobs(name_starts_with=prefix))

    if not blobs:
        print(f"⚠️ No files found in {prefix}")
        return None

    latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]
    return latest_blob.name


# -------------------------------
# Validate blob JSON with schema
# -------------------------------
def validate_blob_json(schema_path, connection_string, container, blob_path):
    try:
        schema = load_json(schema_path)
        data = load_json_from_blob(connection_string, container, blob_path)

        if data is None:
            return

        if isinstance(data, dict):
            validate(instance=data, schema=schema)
        else:
            print(f"ℹ️ Remediation is non-JSON (skipping schema validation)")
            print(f"📝 File content preview: {str(data)[:200]}")
            
            print(f"✅ VALID: {blob_path}")

    except ValidationError as e:
        print(f"❌ INVALID: {blob_path}")
        print("Error:", e.message)

    except Exception as e:
        print(f"⚠️ ERROR: {blob_path} → {e}")


# -------------------------------
# MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":

    # 🔴🔴🔴 REPLACE THIS 🔴🔴🔴
    CONNECTION_STRING = "<YOUR_AZURE_STORAGE_CONNECTION_STRING>"

    # 🔴🔴🔴 VERIFY THIS 🔴🔴🔴
    CONTAINER = "tenant-qnxtupgd"

    # -----------------------
    # Discovery Validation (STATIC - OK)
    # -----------------------
    validate_blob_json(
        "schemas/discovery_schema.json",
        CONNECTION_STRING,
        CONTAINER,
        "agent_handoff/sp_discovery_result.json"   # ✔ keep if guaranteed
    )

    # -----------------------
    # Impact Validation (STATIC - OK)
    # -----------------------
    validate_blob_json(
        "schemas/impact_schema.json",
        CONNECTION_STRING,
        CONTAINER,
        "agent_handoff/sp_impact_report.json"      # ✔ keep if guaranteed
    )

    # -----------------------
    # Remediation Validation (DYNAMIC)
    # -----------------------
    latest_remediation_blob = get_latest_blob(
        CONNECTION_STRING,
        CONTAINER,
        "remediated/"   # 🔴 folder path — verify this
    )

    if latest_remediation_blob:
        print(f"\n🔍 Validating Remediation: {latest_remediation_blob}")
        validate_blob_json(
            "schemas/remediation_schema.json",
            CONNECTION_STRING,
            CONTAINER,
            latest_remediation_blob
        )