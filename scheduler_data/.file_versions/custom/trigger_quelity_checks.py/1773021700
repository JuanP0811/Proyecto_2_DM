if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        dict: response JSON from Mage API trigger
    """
    # Endpoint del trigger API (si te falla por conexión en Docker, cambia localhost -> mage o el nombre del servicio)
    TRIGGER_URL = "http://localhost:6789/api/pipeline_schedules/4/pipeline_runs/434baa984f5b4316bf4f006ffc16af29"

    payload = {
        "pipeline_run": {
            "variables": {
                "triggered_by": "dbt_build_gold"
            }
        }
    }

    print(f"Calling Mage API trigger: {TRIGGER_URL}")
    r = requests.post(TRIGGER_URL, json=payload, timeout=30)
    r.raise_for_status()

    resp = r.json()
    print("Triggered OK:", resp)
    return resp
