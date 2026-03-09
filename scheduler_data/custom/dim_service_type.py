if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def transform_custom(*args, **kwargs):
    import psycopg2
    from mage_ai.data_preparation.shared.secrets import get_secret_value

    host = get_secret_value('POSTGRES_HOST')
    port = int(get_secret_value('POSTGRES_PORT'))
    dbname = get_secret_value('POSTGRES_DB')
    user = get_secret_value('POSTGRES_USER')
    password = get_secret_value('POSTGRES_PASSWORD')

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

    sql = """
    DROP TABLE IF EXISTS gold.dim_service_type_partitioned CASCADE;

    CREATE TABLE gold.dim_service_type_partitioned (
        service_type_key  TEXT NOT NULL,
        service_type_name TEXT,
        PRIMARY KEY (service_type_key)
    ) PARTITION BY LIST (service_type_key);

    CREATE TABLE gold.dim_service_type_partitioned_yellow
    PARTITION OF gold.dim_service_type_partitioned
    FOR VALUES IN ('yellow');

    CREATE TABLE gold.dim_service_type_partitioned_green
    PARTITION OF gold.dim_service_type_partitioned
    FOR VALUES IN ('green');

    INSERT INTO gold.dim_service_type_partitioned (
        service_type_key,
        service_type_name
    )
    SELECT
        service_type_key,
        service_type_name
    FROM gold.dim_service_type;
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM gold.dim_service_type_partitioned;")
    row_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        'status': 'success',
        'table_created': 'gold.dim_service_type_partitioned',
        'rows_inserted': row_count
    }