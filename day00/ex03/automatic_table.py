import os
import psycopg2


def get_all_csv_from(folder_name):
    all_csv = [f for f in os.listdir(folder_name) if f.endswith('.csv')]
    return all_csv


def copy_csv_to_db(csv, folder_name, conn):
    table_name = csv[0: -4]
    print(f"Copying {csv} ...")

    with conn.cursor() as curs:
        try:
            curs.execute(f"DROP TABLE IF EXISTS {table_name}")

            command = f"""CREATE TABLE {table_name} (
                event_time timestamp NOT NULL,
                event_type varchar(255) NOT NULL,
                product_id integer NOT NULL,
                price real NOT NULL,
                user_id bigint NOT NULL,
                user_session uuid
            );"""
            print(f"  Execute... {command}")
            curs.execute(command)

            with open(f"{folder_name}/{csv}", 'r') as f:
                # Notice that we don't need the csv module.
                next(f)  # Skip the header row.
                curs.copy_from(f, table_name, sep=',', null='')

        # a more robust way of handling errors
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    print('Done')


def main():
    folder_name = "./day00/subject/customer"

    # connect db
    try:
        conn = psycopg2.connect(dbname="piscineds", user="alefranc",
                                password="mysecretpassword", host="localhost",
                                port=5432)
    except Exception as e:
        print("I am unable to connect to the database", e)
        exit()

    csv_list = get_all_csv_from(folder_name)
    print(csv_list)
    for csv in csv_list:
        copy_csv_to_db(csv, folder_name, conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
