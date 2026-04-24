import os
import json
from difflib import SequenceMatcher
from pipeline.validation.validator import validate_schema, validate_business_rules, load_json

BASE_PATH = "test_data"

# -------------------------------
# SIMILARITY
# -------------------------------
def similarity(a, b):
    return SequenceMatcher(None, str(a), str(b)).ratio()

# -------------------------------
# LOAD THRESHOLDS
# -------------------------------
def load_thresholds():
    return load_json("regression/thresholds.json")

# -------------------------------
# DRIFT DETECTION
# -------------------------------
def detect_drift(actual, expected, thresholds):
    issues = []

    for field in thresholds["exact_match_fields"]:
        if field in expected:
            if actual.get(field) != expected.get(field):
                issues.append(f"{field} mismatch")

    for field, tol in thresholds["numeric_tolerance"].items():
        if field in expected:
            if abs(actual.get(field, 0) - expected.get(field, 0)) > tol:
                issues.append(f"{field} drift")

    for key, value in actual.items():
        if isinstance(value, str) and key in expected:
            if similarity(value, expected[key]) < thresholds["text_similarity_threshold"]:
                issues.append(f"{key} text drift")

    return issues

# -------------------------------
# RUN TEST CASE
# -------------------------------
def run_test_case(stage, input_file, expected_file, schema_path, rules, thresholds):
    print(f"\n {input_file}")

    data = load_json(input_file)
    expected = load_json(expected_file)

    schema_errors = validate_schema(data, load_json(schema_path))
    business_errors = validate_business_rules(data, stage, rules)

    print("Schema Errors:", schema_errors)
    print("Business Errors:", business_errors)
    
    actual_result = "PASS" if not (schema_errors + business_errors) else "FAIL"
    expected_result = expected["expected"]

    drift = detect_drift(data, expected, thresholds)

    status = " PASS" if actual_result == expected_result else "❌ FAIL"

    print(f"Expected: {expected_result}, Actual: {actual_result} → {status}")

    if drift:
        print(f" Drift: {drift}")

    return {
        "test": input_file,
        "status": status,
        "drift": drift
    }

# -------------------------------
# RUN STAGE
# -------------------------------
def run_stage(stage, schema_path, rules, thresholds):
    print(f"\n===== {stage.upper()} =====")

    folder = os.path.join(BASE_PATH, stage)
    results = []

    if not os.path.exists(folder):
        print(f" Missing folder: {folder}")
        return results

    for file in os.listdir(folder):
        if file.endswith(".json") and not file.endswith("_expected.json"):
            name = file.replace(".json", "")

            input_file = os.path.join(folder, file)
            expected_file = os.path.join(folder, f"{name}_expected.json")

            if not os.path.exists(expected_file):
                print(f"Missing expected: {file}")
                continue

            results.append(
                run_test_case(stage, input_file, expected_file, schema_path, rules, thresholds)
            )

    return results

# -------------------------------
# SUMMARY
# -------------------------------
def summary(results):
    total = sum(len(r) for r in results)
    passed = sum(1 for stage in results for r in stage if "PASS" in r["status"])

    print("\n===== SUMMARY =====")
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}")

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    rules = load_json("pipeline/validation/business_rules.json")["rules"]
    thresholds = load_thresholds()

    discovery = run_stage("discovery", "schemas/discovery_schema.json", rules, thresholds)
    impact = run_stage("impact", "schemas/impact_schema.json", rules, thresholds)
    remediation = run_stage("remediation", "schemas/remediation_schema.json", rules, thresholds)

    summary([discovery, impact, remediation])
    