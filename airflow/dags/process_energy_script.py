import pandas as pd
import json
import requests

def filter_france_emission_production_energy_csv(input_csv_path, output_csv_path):
    type_mapping = {
        "Thermique fossile": "Fossile",
        "Hydraulique": "Hydraulic",
        "Nucléaire": "Nuclear",
        "Eolien": "Wind",
        "Solaire": "Solar"
    }


    df = pd.read_csv(input_csv_path, delimiter=';')


    df.rename(columns={
        'Date': 'year-month',
        'Filière': 'type',
        'Production (TWh)': 'production',
        'Émissions (Mt)': 'emissions'
    }, inplace=True)

    df['type'] = df['type'].replace(type_mapping)


    df['production'] = df['production'].str.replace(',', '.').astype(float)
    df['emissions'] = df['emissions'].str.replace(',', '.').astype(float)


    df.to_csv(output_csv_path, index=False)

def join_france_emission_energy_csv(production_csv_path, emission_csv_path, output_csv_path):
    production_data = pd.read_csv(production_csv_path, sep=';', decimal=',')
    emission_data = pd.read_csv(emission_csv_path, sep=';', decimal=',')

    production_data['Date'] = pd.to_datetime(production_data['Date'], format='%Y-%m').dt.to_period('M').astype(str)
    emission_data['Date'] = pd.to_datetime(emission_data['Date'], format='%Y-%m').dt.to_period('M').astype(str)

    emission_data['Filière'] = 'Thermique fossile'

    emission_data = emission_data.rename(columns={'Valeur (Mt)': 'Émissions (Mt)'})

    merged_data = pd.merge(
        production_data,
        emission_data[['Date', 'Filière', 'Émissions (Mt)']],
        on=['Date', 'Filière'],
        how='left'
    )
    merged_data['Émissions (Mt)'] = merged_data['Émissions (Mt)'].fillna(0)


    merged_data = merged_data.rename(columns={'Valeur (TWh)': 'Production (TWh)'})


    merged_data.to_csv(output_csv_path, sep=';', index=False, decimal=',')


def process_france_production_energy_csv(input_csv_path, output_csv_path):
    df = pd.read_csv(input_csv_path, sep=';')

 
    if 'Nature' in df.columns:
        df = df.drop(columns=['Nature'])


    filtres = ['Production totale', 'Thermique renouvelable et déchets']
    df = df[~df['Filière'].isin(filtres)]

    df.to_csv(output_csv_path, sep=';', index=False)

def process_france_emission_energy_csv(input_file, output_file):
    
    data = pd.read_csv(input_file, sep=';', decimal=',')

    filtered_data = data[~data['Filière'].isin(["Déchets ménagers", "Emission de gaz à effet de serre"])]

    filtered_data['Type'] = "Thermique fossile"

    filtered_data = filtered_data.drop(columns=["Nature"])

    total_emissions = filtered_data.groupby(["Date", "Type"])["Valeur (Mt)"].sum().reset_index()

    total_emissions.to_csv(output_file, sep=';', index=False, decimal=',', encoding='utf-8')

def process_production_emission_germany(csv_input_file, csv_output_file):
    type_mapping = {
        "Fossil brown coal / lignite": "Fossile",
        "Fossil hard coal": "Fossile",
        "Fossil oil": "Fossile",
        "Fossil coal-derived gas": "Fossile",
        "Fossil gas": "Fossile",
        "Hydro Run-of-River": "Hydraulic",
        "Hydro pumped storage": "Hydraulic",
        "Nuclear": "Nuclear",
        "Wind onshore": "Wind",
        "Wind offshore": "Wind",
        "Solar": "Solar"
    }

    
    df = pd.read_csv(csv_input_file)

    
    df['type'] = df['type'].map(type_mapping)

    
    df = df.dropna(subset=['type'])

    df_grouped = df.groupby(['year-month', 'type'], as_index=False).agg({'production': 'sum', 'emissions': 'sum'})

    df_grouped.to_csv(csv_output_file, index=False)

def join_production_emission_germany_data(csv_emission_file, csv_production_file, csv_output_file):
    df_emissions = pd.read_csv(csv_emission_file)
    df_production = pd.read_csv(csv_production_file)

    df_emissions['year-month'] = pd.to_datetime(df_emissions['year-month'], format='%Y-%m').dt.to_period('M')
    df_production['year_month'] = pd.to_datetime(df_production['year_month'], format='%Y-%m').dt.to_period('M')

    df_merged = pd.merge(df_production, df_emissions, left_on=['year_month', 'type'], right_on=['year-month', 'type'], how='left')

    df_merged['emissions'].fillna(0, inplace=True)

    df_merged = df_merged[['year_month', 'type', 'production', 'emissions']]

    df_merged.rename(columns={'year_month': 'year-month'}, inplace=True)
    df_merged['year-month'] = df_merged['year-month'].astype(str)

    df_merged["production"] = (df_merged["production"] / 1_000_000).round(3)

    df_merged.to_csv(csv_output_file, index=False)

def process_emission_germany_monthly(csv_input_file, csv_output_file):

    df = pd.read_csv(csv_input_file)
    df["monthly_emissions"] = (df["emissions"] / 12).round(2)


    months = [f"{month:02}" for month in range(1, 13)]
    rows = []

    for _, row in df.iterrows():
        for month in months:
            rows.append({
                "year-month": f"{row['year']}-{month}",
                "type": row["type"],
                "emissions": row["monthly_emissions"]
            })

    df_monthly = pd.DataFrame(rows)
    df_monthly.to_csv(csv_output_file, index=False)

def process_emission_germany(csv_input_file, csv_output_file):
    data = pd.read_csv(csv_input_file, skiprows=1)

    data.columns = ["Year", "Fossil brown coal / lignite", "Fossil hard coal", "Fossil gas", "Fossil oil", "Waste (fossil)", "Others"]

    melted_data = data.melt(id_vars=["Year"], var_name="Type", value_name="Emissions")

    melted_data = melted_data.rename(columns={"Year": "year", "Type": "type", "Emissions": "emissions"})

    melted_data.to_csv(csv_output_file, index=False)

    



def aggregating_german_data(csv_input_file, csv_output_file):
    df = pd.read_csv(csv_input_file)


    type_mapping = {
        "Fossil brown coal / lignite": "Fossile",
        "Fossil hard coal": "Fossile",
        "Fossil oil": "Fossile",
        "Fossil coal-derived gas": "Fossile",
        "Fossil gas": "Fossile",
        "Hydro Run-of-River": "Hydraulic",
        "Hydro pumped storage": "Hydraulic",
        "Nuclear": "Nuclear"
    }

    
    df['type'] = df['type'].map(type_mapping)

    
    df = df[df['type'].isin(["Fossile", "Hydraulic", "Nuclear"])]

   
    df['production'] = (df['production'] / 1000).round(2)

   
    result = df.groupby(['year', 'type'], as_index=False)['production'].sum()
    result.to_csv(csv_output_file, index=False)

def save_json_to_file(api_url, json_file):

    response = requests.get(api_url)
    response.raise_for_status()

   
    data = response.json()

    
    with open(json_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to {json_file}")

def process_german_data(json_data_path, output_csv):
    
    with open(json_data_path, 'r') as file:
        json_data = json.load(file)

    timestamps = pd.to_datetime(json_data["unix_seconds"], unit="s")
    year_month = timestamps.to_period("M")  

    result = pd.DataFrame()

    for production_type in json_data["production_types"]:
        name = production_type["name"]
        data = production_type["data"]

  
        df = pd.DataFrame({"year_month": year_month, "production": data})
        df = df.groupby("year_month", as_index=False)["production"].sum()
        df["type"] = name

   
        result = pd.concat([result, df], ignore_index=True)


    result.to_csv(output_csv, index=False)