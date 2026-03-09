-- Docimport os
import subprocess

def run_dbt():
    project_dir = "/home/src/dbt_project"
    profiles_dir = "/root/.dbt"  # aquí está tu profiles.yml que ya funciona

    cmd = [
        "dbt", "run",
        "--select", "models/silver",
        "--target", "dev",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
    ]

    env = os.environ.copy()
    # (opcional) fuerza a dbt a usar ese directorio
    env["DBT_PROFILES_DIR"] = profiles_dir

    result = subprocess.run(
        cmd,
        cwd=project_dir,
        env=env,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt failed with code {result.returncode}")

run_dbt()s: https://docs.mage.ai/guides/sql-blocks
