import os
import subprocess

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    Runs dbt to build Silver models.

    This avoids Mage's DBT block profiles interpolation issue by calling dbt directly.
    """
    project_dir = "/home/src/dbt_project"
    profiles_dir = "/root/.dbt"  # contains /root/.dbt/profiles.yml
    target = "dev"
    select_path = "models/silver"

    cmd = [
        "dbt", "run",
        "--select", select_path,
        "--target", target,
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
    ]

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = profiles_dir

    res = subprocess.run(
        cmd,
        cwd=project_dir,
        env=env,
        capture_output=True,
        text=True,
    )

    # Print logs into Mage block output
    stdout = res.stdout or ""
    stderr = res.stderr or ""
    print(stdout)
    if stderr.strip():
        print(stderr)

    if res.returncode != 0:
        raise Exception(f"dbt run failed (code={res.returncode}). See logs above.")

    # Return something small + useful for the next blocks / debugging
    return {
        "status": "success",
        "command": " ".join(cmd),
        "project_dir": project_dir,
        "profiles_dir": profiles_dir,
        "target": target,
        "select": select_path,
    }


@test
def test_output(output, *args) -> None:
    assert output is not None, "The output is undefined"
    assert isinstance(output, dict), "Expected dict output"
    assert output.get("status") == "success", "dbt did not complete successfully"