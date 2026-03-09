if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom


@custom
def transform_custom(*args, **kwargs):
    import psycopg2
    from datetime import date
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

    cur = conn.cursor()

    try:
        print(" Eliminando tabla previa si existe...")
        cur.execute("DROP TABLE IF EXISTS gold.fct_trips_partitioned CASCADE;")

        print(" Creando tabla padre particionada...")
        create_parent_sql = """
        CREATE TABLE gold.fct_trips_partitioned (
            trip_key TEXT NOT NULL,
            pickup_date_key DATE NOT NULL,
            dropoff_date_key DATE,
            pu_zone_key INTEGER,
            do_zone_key INTEGER,
            service_type_key TEXT,
            payment_type_key INTEGER,
            vendor_key INTEGER,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            passenger_count NUMERIC,
            trip_distance NUMERIC,
            trip_duration_min NUMERIC,
            fare_amount NUMERIC,
            extra NUMERIC,
            mta_tax NUMERIC,
            tip_amount NUMERIC,
            tolls_amount NUMERIC,
            improvement_surcharge NUMERIC,
            congestion_surcharge NUMERIC,
            airport_fee NUMERIC,
            ehail_fee NUMERIC,
            total_amount NUMERIC,
            tip_pct NUMERIC,
            avg_speed_mph NUMERIC,
            ratecodeid NUMERIC,
            store_and_fwd_flag TEXT,
            trip_type NUMERIC,
            source_month TEXT,
            PRIMARY KEY (trip_key, pickup_date_key)
        ) PARTITION BY RANGE (pickup_date_key);
        """
        cur.execute(create_parent_sql)

        print(" Detectando rango de fechas...")
        cur.execute("""
            SELECT
                MIN(pickup_date_key)::date AS min_date,
                MAX(pickup_date_key)::date AS max_date
            FROM gold.fct_trips
            WHERE pickup_date_key IS NOT NULL;
        """)
        result = cur.fetchone()
        min_date, max_date = result

        if min_date is None or max_date is None:
            raise Exception("No existen fechas válidas en gold.fct_trips.")

        print(f" Rango detectado: {min_date} -> {max_date}")

        start_date = date(min_date.year, min_date.month, 1)
        end_date = date(max_date.year, max_date.month, 1)

        current = start_date

        print(" Creando particiones mensuales...")
        while current <= end_date:
            if current.month == 12:
                next_month = date(current.year + 1, 1, 1)
            else:
                next_month = date(current.year, current.month + 1, 1)

            partition_name = f"fct_trips_partitioned_y{current.year}m{current.month:02d}"

            partition_sql = f"""
            CREATE TABLE gold.{partition_name}
            PARTITION OF gold.fct_trips_partitioned
            FOR VALUES FROM ('{current}') TO ('{next_month}');
            """
            cur.execute(partition_sql)

            current = next_month

        print(" Insertando datos desde gold.fct_trips...")
        insert_sql = """
        INSERT INTO gold.fct_trips_partitioned (
            trip_key,
            pickup_date_key,
            dropoff_date_key,
            pu_zone_key,
            do_zone_key,
            service_type_key,
            payment_type_key,
            vendor_key,
            pickup_ts,
            dropoff_ts,
            passenger_count,
            trip_distance,
            trip_duration_min,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            airport_fee,
            ehail_fee,
            total_amount,
            tip_pct,
            avg_speed_mph,
            ratecodeid,
            store_and_fwd_flag,
            trip_type,
            source_month
        )
        SELECT
            trip_key,
            pickup_date_key,
            dropoff_date_key,
            pu_zone_key,
            do_zone_key,
            service_type_key,
            payment_type_key,
            vendor_key,
            pickup_ts,
            dropoff_ts,
            passenger_count,
            trip_distance,
            trip_duration_min,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            airport_fee,
            ehail_fee,
            total_amount,
            tip_pct,
            avg_speed_mph,
            ratecodeid,
            store_and_fwd_flag,
            trip_type,
            source_month
        FROM gold.fct_trips
        WHERE pickup_date_key IS NOT NULL;
        """
        cur.execute(insert_sql)

        print(" Contando filas insertadas...")
        cur.execute("SELECT COUNT(*) FROM gold.fct_trips_partitioned;")
        row_count = cur.fetchone()[0]

        conn.commit()

        return {
            'status': 'success',
            'table_created': 'gold.fct_trips_partitioned',
            'rows_inserted': row_count,
            'min_date': str(min_date),
            'max_date': str(max_date)
        }

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()