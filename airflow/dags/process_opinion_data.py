import pandas as pd

def process_and_aggregate_opinion(csv_file,output_csv_path):

    df = pd.read_csv(csv_file)

    df['typeenergie'] = df['typeenergie'].replace({
        'fossile': 'fossile',
        'charbon': 'fossile',
        'eolien': 'eolien',
        'eolienne': 'eolien',
        'kohle': 'fossil',
        'fossil': 'fossil',
        'windkraft': 'wind',
        'windturbine': 'wind'
    })

    aggregated_df = df.groupby(['year-month', 'typeenergie']).agg({
        'negative': 'sum',
        'positif': 'sum',
        'neutre': 'sum'
    }).reset_index()

    aggregated_df.to_csv(output_csv_path, index=False)

def add_country_column(csv_file,output_csv_path):

    df = pd.read_csv(csv_file)

    energy_to_country = {
        'fossil': 'Germany',
        'kernenergie': 'Germany',
        'solarenergie': 'Germany',
        'wind': 'Germany',
        'wasserkraft': 'Germany',
        'solaire': 'France',
        'fossile': 'France',
        'nucléaire': 'France',
        'hydraulique': 'France',
        'eolien': 'France'
    }


    df['country'] = df['typeenergie'].map(energy_to_country)

    df.to_csv(output_csv_path, index=False)

def transform_and_rename(csv_file,output_csv_path):
    
    df = pd.read_csv(csv_file)


    energy_type_transformation = {
        'fossil': 'Fossile',
        'kernenergie': 'Nuclear',
        'solarenergie': 'Solar',
        'wind': 'Wind',
        'wasserkraft': 'Hydraulique',
        'solaire': 'Solar',
        'fossile': 'Fossile',
        'nucléaire': 'Nuclear',
        'hydraulique': 'Hydraulique',
        'eolien': 'Wind'
    }

  
    df['typeenergie'] = df['typeenergie'].replace(energy_type_transformation)

    df.rename(columns={'typeenergie': 'type'}, inplace=True)

    df.to_csv(output_csv_path, index=False)