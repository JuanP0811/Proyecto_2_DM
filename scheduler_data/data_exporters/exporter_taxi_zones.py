import psycopg2
from psycopg2.extras import execute_values

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def _secret(name: str, default=None):
    """
    Lee Mage Secrets; si no existe, usa default.
    """
    val = get_secret_value(name)
    return val if val not in (None, "") else default


def _get_conn():
    host = _secret("POSTGRES_HOST", "warehouse")
    port = int(_secret("POSTGRES_PORT", "5432"))
    dbname = _secret("POSTGRES_DB")
    user = _secret("POSTGRES_USER")
    password = _secret("POSTGRES_PASSWORD")

    missing = [k for k, v in {
        "POSTGRES_DB": dbname,
        "POSTGRES_USER": user,
        "POSTGRES_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise ValueError(
            f"Missing Mage Secrets for Postgres connection: {missing}. "
            f"Go to Mage UI -> Secrets and create them."
        )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta Taxi Zone Lookup a Postgres (bronze):
      - Crea schema bronze si no existe
      - Crea tabla bronze.taxi_zone_lookup si no existe
      - TRUNCATE (idempotente)
      - INSERT masivo (rápido)
    """
    if data is None:
        raise ValueError("No data received from upstream block.")
    if not isinstance(data, list):
        raise TypeError(f"Expected 'data' to be a list[dict], got: {type(data)}")
    if len(data) > 0 and not isinstance(data[0], dict):
        raise TypeError("Expected 'data' as list[dict] rows.")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # 1) Schema + table
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze.taxi_zone_lookup (
                    location_id   INTEGER PRIMARY KEY,
                    borough       TEXT,
                    zone          TEXT,
                    service_zone  TEXT,
                    ingest_ts     TIMESTAMP
                );
            """)

            # 2) Idempotencia
            cur.execute("TRUNCATE TABLE bronze.taxi_zone_lookup;")

            # 3) Bulk insert
            rows = [
                (
                    r.get("location_id"),
                    r.get("borough"),
                    r.get("zone"),
                    r.get("service_zone"),
                    r.get("ingest_ts"),
                )
                for r in data
            ]

            execute_values(
                cur,
                """
                INSERT INTO bronze.taxi_zone_lookup
                    (location_id, borough, zone, service_zone, ingest_ts)
                VALUES %s
                """,
                rows,
                page_size=1000,
            )

        conn.commit()
        return {"rows_loaded": len(data), "table": "bronze.taxi_zone_lookup"}
    finally:
        conn.close()
