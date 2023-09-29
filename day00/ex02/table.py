import psycopg2


def main():
    # Connect to your postgres DB
    try:
        conn = psycopg2.connect(dbname="piscineds", user="alefranc",
                                password="mysecretpassword", host="localhost",
                                port=5432)
    except Exception as e:
        print("I am unable to connect to the database", e)

    with conn.cursor() as curs:
        try:
            curs.execute("DROP TABLE IF EXISTS data_2022_oct")

            curs.execute("""CREATE TABLE data_2022_oct (
                event_time timestamp NOT NULL,
                event_type varchar(255) NOT NULL,
                product_id integer NOT NULL,
                price real NOT NULL,
                user_id bigint NOT NULL,
                user_session uuid
            );""")

            with open('./day00/subject/customer/data_2022_oct.csv', 'r') as f:
                # Notice that we don't need the csv module.
                next(f)  # Skip the header row.
                curs.copy_from(f, 'data_2022_oct', sep=',', null='')

        # a more robust way of handling errors
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
