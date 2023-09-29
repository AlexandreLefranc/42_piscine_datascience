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


def remove_duplicates(conn):
    print("Remove duplicates")

    execute_sql(conn, """
    DELETE FROM customers
    WHERE ctid IN (SELECT ctid
        FROM   (SELECT ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY event_time, event_type,
                            product_id, price, user_id, user_session) AS rn
                FROM   customers) t
        WHERE  rn > 1);
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

    remove_duplicates(conn)

    print("Commit changes")
    conn.commit()
    print("Close connection")
    conn.close()


if __name__ == "__main__":
    main()
