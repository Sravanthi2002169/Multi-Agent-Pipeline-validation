def get_schema(stage):
    """
    Returns schema path for each stage
    """
    schema_map = {
        "discovery": "schemas/discovery_schema.json",
        "planning": "schemas/impact_schema.json",
        "remediation": "schemas/remediation_schema.json"
    }
    
    return schema_map.get(stage)