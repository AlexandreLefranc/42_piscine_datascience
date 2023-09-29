import psycopg2


def main():

    # connect db
    try:
        conn = psycopg2.connect(dbname="piscineds", user="alefranc",
                                password="mysecretpassword", host="localhost",
                                port=5432)
    except Exception as e:
        print("I am unable to connect to the database:", e)
        exit()

    file_name = "./day00/subject/item/item.csv"
    with conn.cursor() as curs:
        try:
            curs.execute("DROP TABLE IF EXISTS items")

            curs.execute("""CREATE TABLE items (
                product_id integer NOT NULL,
                category_id bigint,
                category_code text,
                brand varchar(255)
            );""")

            with open(file_name, 'r') as f:
                # Notice that we don't need the csv module.
                next(f)  # Skip the header row.
                curs.copy_from(f, 'items', sep=',', null='')

        # a more robust way of handling errors
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
