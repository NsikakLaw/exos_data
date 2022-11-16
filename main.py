import pandas as pd
import json
import os
from create_database import Database, create_sql_statement

with open('schema.json', 'r') as f:
    schema_dict = json.load(f)


def clean_dollar_string(value):
    try:
        dollar_float = float(value.replace("$", "").replace(",", ""))
    except Exception as e:
        return None

    return dollar_float


def convert_csv_to_df(filename):
    df = pd.read_csv(filename)
    df['average_salary_per_film'] = df.apply(lambda x: clean_dollar_string(x['average_salary_per_film']), axis=1)
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['death_date'] = pd.to_datetime(df['death_date'], errors='coerce')
    return list(df.columns), df


def create_db_details(table_name, schema, csv_file):
    new_db = Database("test.db")
    create_statement = create_sql_statement(table_name, schema)
    new_db.create_table(create_statement)

    table_df = convert_csv_to_df(csv_file)

    try:
        table_df[1].to_sql(table_name, con=new_db.db_connection, if_exists='append', dtype=schema)
    except Exception as e:
        print(e)
    new_db.close_connection()


def get_top_deceased_salary(db):
    sql_query = "select name from actors where " \
                "strftime('%Y',death_date) IS NOT NULL order by average_salary_per_film DESC limit 10;"

    c = db.cursor()
    c.execute(sql_query)
    result = [i[0] for i in c.fetchall()]
    c.close()
    return result


def get_number_of_cinematographers(db):
    sql_query = "select count(id) from actors where primary_profession like '%cinematographer%';"
    c = db.cursor()
    c.execute(sql_query)
    result = c.fetchall()[0][0]
    c.close()
    return result


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if not os.path.isfile('./test.db'):
        create_db_details("actors", schema_dict, "csv/actors.csv")

    n_db = Database("test.db")

    print(get_top_deceased_salary(n_db.db_connection))
    print(get_number_of_cinematographers(n_db.db_connection))
    n_db.close_connection()
