from psycopg2 import sql
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_csv_opinion_to_staging_db(csv_file):
    
    df = pd.read_csv(csv_file)


    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()


    insert_query = sql.SQL("""
        INSERT INTO opinion_data (year_month, type, positive_opinion, neutral_opinion, negative_opinion, country)
        VALUES (%s, %s, %s, %s, %s, %s)
    """)

    
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['year-month'],
            row['type'],
            row['positif'],  
            row['neutre'],   
            row['negative'],
            row['country']      
        ))

    conn.commit()
    cursor.close()
    conn.close()

def load_csv_france_to_staging_db(csv_file):
    
    df = pd.read_csv(csv_file)


    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()


    insert_query = sql.SQL("""
        INSERT INTO energy_france_data (year_month, type, energy_production, energy_emission)
        VALUES (%s, %s, %s, %s)
    """)

    
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['year-month'],
            row['type'],
            row['production'],  
            row['emissions'],       
        ))

    conn.commit()
    cursor.close()
    conn.close()

def load_csv_germany_to_staging_db(csv_file):
    
    df = pd.read_csv(csv_file)


    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()


    insert_query = sql.SQL("""
        INSERT INTO energy_germany_data (year_month, type, energy_production, energy_emission)
        VALUES (%s, %s, %s, %s)
    """)

    
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['year-month'],
            row['type'],
            row['production'],  
            row['emissions'],       
        ))

    conn.commit()
    cursor.close()
    conn.close()


def enrich_emission_data():
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query = "SELECT * FROM energy_aggregated_data"

    data = pd.read_sql(query, conn)

    emission_factors = {
        "Nuclear": 0.012,
        "Solar": 0.041,
        "Hydraulic": 0.024,
        "Wind": 0.011,
        "Fossile": None  
    }

    def calculate_emissions(row):
        energy_type = row["type"]
        production_twh = row["energy_production"] 
        
        if energy_type in emission_factors and emission_factors[energy_type] is not None:
            return production_twh * emission_factors[energy_type]
        return row["energy_emission"]  

    data["energy_emission"] = data.apply(calculate_emissions, axis=1).round(3)

    data["energy_production"] = data["energy_production"].round(3)
    
    insert_query = sql.SQL("""
        INSERT INTO energy_enriched_data (year_month, type, energy_production, energy_emission, country)
        VALUES (%s, %s, %s, %s, %s)
    """)

    for _, row in data.iterrows():
        cursor.execute(insert_query, (
            row['year_month'],
            row['type'],
            row['energy_production'],  
            row['energy_emission'],   
            row['country']     
        ))

    conn.commit()
    cursor.close()
    conn.close()

    

def load_energy_data_to_postgres():

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    pg_hook2 = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn2 = pg_hook2.get_conn()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query = "SELECT * FROM energy_opinion_data"

    data = pd.read_sql(query, conn2)

    
  
    for energy_type in data['type'].unique():
        cursor.execute(
            """
            INSERT INTO Energy_Type_Dim (type_name, renewable_flag)
            VALUES (%s, %s)
            ON CONFLICT (type_name) DO NOTHING;
            """,
            (energy_type, energy_type.lower() in ['solar', 'wind', 'hydro'])
        )

    for country in data['country'].unique():
        cursor.execute(
            """
            INSERT INTO Country_Dim (country_name)
            VALUES (%s)
            ON CONFLICT (country_name) DO NOTHING;
            """,
            (country,)
        )


    for index, row in data.iterrows():
        year, month = row['year_month'].split('-')
        cursor.execute(
            """
            INSERT INTO Time_Dim (month, year)
            VALUES (%s, %s)
            ON CONFLICT (month, year) DO NOTHING;
            """,
            (int(month), int(year))
        )


    for index, row in data.iterrows():
        year, month = row['year_month'].split('-')
        
    
        cursor.execute(
            """
            SELECT time_id FROM Time_Dim
            WHERE month = %s AND year = %s;
            """,
            (int(month), int(year))
        )
        time_id = cursor.fetchone()[0]


        emissions = row['energy_emission']
        energy_efficiency = round(row['energy_production'] / (emissions if emissions > 0 else 1), 3)

        cursor.execute(
            """
            INSERT INTO Energy_Impact (
                energy_type_id, country_id, time_id,
                neutral_opinion, negative_opinion, positive_opinion,
                energy_production, energy_emission, energy_efficiency
            )
            SELECT
                (SELECT energy_type_id FROM Energy_Type_Dim WHERE type_name = %s),
                (SELECT country_id FROM Country_Dim WHERE country_name = %s),
                %s,
                %s, %s, %s,
                %s, %s, %s
            """,
            (
                row['type'], row['country'], time_id,
                row['neutral_opinion'], row['negative_opinion'],
                row['positive_opinion'],
                row['energy_production'], emissions, energy_efficiency
            )
        )

    conn.commit()
    cursor.close()
    conn.close()


def join_opinion_and_energy_data():

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query_energy = "SELECT * FROM energy_enriched_data"

    query_opinion = "SELECT * FROM opinion_data"

    df1 = pd.read_sql(query_energy, conn)
    df2 = pd.read_sql(query_opinion, conn)


    merged_df = pd.merge(df1, df2, on=['year_month', 'type', 'country'], how='left')


    merged_df['positive_opinion'] = merged_df['positive_opinion'].fillna(0).astype(int)
    merged_df['neutral_opinion'] = merged_df['neutral_opinion'].fillna(0).astype(int)
    merged_df['negative_opinion'] = merged_df['negative_opinion'].fillna(0).astype(int)


    merged_df['energy_emission'] = merged_df['energy_emission'].fillna(0.0)
    merged_df['energy_production'] = merged_df['energy_production'].fillna(0.0)


    insert_query = sql.SQL("""
        INSERT INTO energy_opinion_data (year_month, type, energy_production, energy_emission, positive_opinion, neutral_opinion, negative_opinion, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)

    
    for _, row in merged_df.iterrows():
        cursor.execute(insert_query, (
            row['year_month'],
            row['type'],
            row['energy_production'],  
            row['energy_emission'],   
            row['positive_opinion'],   
            row['neutral_opinion'],
            row['negative_opinion'],      
            row['country']     
        ))

    conn.commit()
    cursor.close()
    conn.close()

def merge_energy_data():

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query_france = "SELECT * FROM energy_france_data"
    df1 = pd.read_sql(query_france, conn)
    df1['country'] = 'France'

    query_germany = "SELECT * FROM energy_germany_data"
    df2 = pd.read_sql(query_germany, conn)
    df2['country'] = 'Germany'

    df_combined = pd.concat([df1, df2], ignore_index=True)


    insert_query = sql.SQL("""
        INSERT INTO energy_aggregated_data (year_month, type, energy_production, energy_emission, country)
        VALUES (%s, %s, %s, %s, %s)
    """)

    
    for _, row in df_combined.iterrows():
        cursor.execute(insert_query, (
            row['year_month'],
            row['type'],
            row['energy_production'],  
            row['energy_emission'],   
            row['country']     
        ))

    conn.commit()
    cursor.close()
    conn.close()

def clear_staging_tables():
    tables_to_clear = ['opinion_data', 'energy_aggregated_data', 'energy_france_data', 'energy_germany_data', 'energy_enriched_data','energy_opinion_data']
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_staging')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        for table in tables_to_clear:
            truncate_query = f"TRUNCATE TABLE {table}"
            cursor.execute(truncate_query)
        conn.commit()
        print(f"Les tables {', '.join(tables_to_clear)} ont été vidées avec succès.")
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la suppression des données : {e}")
    finally:
        cursor.close()
        conn.close()

def clear_production_tables():
    tables_to_clear = ['Energy_Type_Dim', 'Country_Dim', 'Time_Dim', 'Source_Dim', 'Energy_Impact']
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        for table in tables_to_clear:
            truncate_query = f"TRUNCATE TABLE {table} CASCADE"
            cursor.execute(truncate_query)
        conn.commit()
        print(f"Les tables {', '.join(tables_to_clear)} ont été vidées avec succès.")
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la suppression des données : {e}")
    finally:
        cursor.close()
        conn.close()

def clear_all_tables():
    clear_production_tables()
    clear_staging_tables()
