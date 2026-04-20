import logging
import json
import azure.functions as func

from validators.validator import run_validation   # 👈 import your logic

def main(myblob: func.InputStream):
    logging.info(f"📥 Blob trigger fired: {myblob.name}")

    content = myblob.read().decode("utf-8")

    try:
        data = json.loads(content)
    except:
        data = content

    # 👇 call your main validation logic
    result = run_validation(data, myblob.name)

    logging.info(f"📊 RESULT: {result}")