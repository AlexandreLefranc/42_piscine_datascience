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


def join_tables(conn):
    print("Join tables")

    execute_sql(conn, "DROP TABLE IF EXISTS customers")
    execute_sql(conn, """
    CREATE TABLE customers AS
        (SELECT * FROM data_2022_oct
        UNION ALL
        SELECT * FROM data_2022_nov
        UNION ALL
        SELECT * FROM data_2022_dec
        UNION ALL
        SELECT * FROM data_2023_jan)
    """)


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

    join_tables(conn)

    print("Commit changes")
    conn.commit()
    print("Close connection")
    conn.close()


if __name__ == "__main__":
    main()
