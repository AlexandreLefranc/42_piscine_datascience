import psycopg2


def execute_sql(conn, command):
    print(f"  Executing... {command}")

    try:
        with conn.cursor() as curs:
            curs.execute(command)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.close()
        exit()

    print("Done!")


def fusion(conn):
    print("fusion")

    execute_sql(conn, "DROP TABLE IF EXISTS customers_tmp;")

    execute_sql(conn, """
    CREATE TABLE customers_tmp
    AS (
        SELECT
            c.event_time,
            c.event_type,
            c.product_id,
            i.category_id,
            i.category_code,
            i.brand,
            c.price,
            c.user_id,
            c.user_session
        FROM customers c
        FULL JOIN items i ON c.product_id = i.product_id
    );
    """)

    execute_sql(conn, "DROP TABLE customers;")

    execute_sql(conn, "ALTER TABLE customers_tmp RENAME TO customers;")


def main():
    # connect db
    try:
        conn = psycopg2.connect(dbname="piscineds", user="alefranc",
                                password="mysecretpassword", host="localhost",
                                port=5432)
        print("Connected to PostgreSQL!")
    except Exception as e:
        print("I am unable to connect to the database", e)
        exit()

    fusion(conn)

    print("Commit changes")
    conn.commit()
    print("Close connection")
    conn.close()


if __name__ == "__main__":
    main()
