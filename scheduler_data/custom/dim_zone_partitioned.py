if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def transform_custom(*args, **kwargs):
    import psycopg2
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    # Obtener secretos desde Mage
    host = get_secret_value('POSTGRES_HOST')
    port = int(get_secret_value('POSTGRES_PORT'))
    dbname = get_secret_value('POSTGRES_DB')
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')

    # Conexión a Postgres
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

    sql = """
    DROP TABLE IF EXISTS gold.dim_zone_partitioned CASCADE;

    CREATE TABLE gold.dim_zone_partitioned (
        zone_key     INT NOT NULL,
        borough      TEXT,
        zone         TEXT,
        service_zone TEXT,
        PRIMARY KEY (zone_key)
    ) PARTITION BY HASH (zone_key);

    CREATE TABLE gold.dim_zone_partitioned_p0
    PARTITION OF gold.dim_zone_partitioned
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);

    CREATE TABLE gold.dim_zone_partitioned_p1
    PARTITION OF gold.dim_zone_partitioned
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);

    CREATE TABLE gold.dim_zone_partitioned_p2
    PARTITION OF gold.dim_zone_partitioned
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);

    CREATE TABLE gold.dim_zone_partitioned_p3
    PARTITION OF gold.dim_zone_partitioned
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

    INSERT INTO gold.dim_zone_partitioned (
        zone_key,
        borough,
        zone,
        service_zone
    )
    SELECT
        zone_key,
        borough,
        zone,
        service_zone
    FROM gold.dim_zone;
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM gold.dim_zone_partitioned;")
    row_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        'status': 'success',
        'table_created': 'gold.dim_zone_partitioned',
        'rows_inserted': row_count
    }