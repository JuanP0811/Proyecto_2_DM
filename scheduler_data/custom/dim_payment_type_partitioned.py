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
    DROP TABLE IF EXISTS gold.dim_payment_type_partitioned CASCADE;

    CREATE TABLE gold.dim_payment_type_partitioned (
        payment_type_key  INTEGER NOT NULL,
        payment_type_name TEXT NOT NULL,
        PRIMARY KEY (payment_type_key)
    ) PARTITION BY LIST (payment_type_key);

    CREATE TABLE gold.dim_payment_type_partitioned_unknown_zero
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (0);

    CREATE TABLE gold.dim_payment_type_partitioned_cash
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (1);

    CREATE TABLE gold.dim_payment_type_partitioned_card
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (2);

    CREATE TABLE gold.dim_payment_type_partitioned_no_charge
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (3);

    CREATE TABLE gold.dim_payment_type_partitioned_dispute
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (4);

    CREATE TABLE gold.dim_payment_type_partitioned_unknown
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (5);

    CREATE TABLE gold.dim_payment_type_partitioned_voided_trip
    PARTITION OF gold.dim_payment_type_partitioned
    FOR VALUES IN (6);

    INSERT INTO gold.dim_payment_type_partitioned (
        payment_type_key,
        payment_type_name
    )
    SELECT
        payment_type_key,
        payment_type_name
    FROM gold.dim_payment_type;
    """

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM gold.dim_payment_type_partitioned;")
    row_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        'status': 'success',
        'table_created': 'gold.dim_payment_type_partitioned',
        'rows_inserted': row_count
    }