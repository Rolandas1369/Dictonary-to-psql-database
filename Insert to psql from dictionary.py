import psycopg2

password = ''
database = ''


def create_table_from_dict(table_name, dictionary, user='postgres', password=password, host="127.0.0.1", port="5432",
                           database=database):
    rows = ",".join(dictionary.keys())
    values = [i for i in dictionary.values()]
    args_number = "".join(len(dictionary) * '%s,')

    try:
        # connect
        connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = connection.cursor()

        # select if table table exists
        postgres_table_exist_query = """SELECT EXISTS(
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = '""" + table_name + """');"""
        cursor.execute(postgres_table_exist_query)
        does_exists = cursor.fetchone()

        # create table if doesn't exist
        if does_exists[0] == True:
            pass
        else:
            postgres_create_table = """CREATE TABLE """ + table_name + """();"""
            cursor.execute(postgres_create_table)
            print(table_name, ", was created.")

        # check for existing rows
        postgres_select_query = """ SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = '""" + table_name + """';"""
        cursor.execute(postgres_select_query)

        selected_rows = cursor.fetchall()
        rows_list = []
        if len(selected_rows) == 0:
            pass
        else:
            for row in selected_rows:
                rows_list.append(row[0])

        # rows to insert from dictionary keys
        insertable_rows = rows.split(',')
        missing_rows = [item for item in insertable_rows if item.lower() not in rows_list]

        if len(missing_rows) == 0:

            # inserting values from dictionary
            postgres_insert_query = """ INSERT INTO """ + table_name + """ (""" + rows + """) VALUES (""" + args_number[
                                                                                                            :-1] + """)"""
            cursor.execute(postgres_insert_query, values)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        else:

            # inserting missing keys from dictionary
            for row in missing_rows:
                postgres_create_rows_query = """ ALTER TABLE """ + table_name + """
                ADD """ + row + """ varchar(255);"""
                cursor.execute(postgres_create_rows_query)
            print("Missing row('s):", missing_rows, "inserted")

            # inserting values from dictionary
            postgres_insert_query = """ INSERT INTO """ + table_name + """ (""" + rows + """) VALUES (""" + args_number[
                                                                                                            :-1] + """)"""
            cursor.execute(postgres_insert_query, values)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


table_name='test2'
dictionary={'Sample':'one', 'Another': 'sample'}
create_table_from_dict(table_name, dictionary)