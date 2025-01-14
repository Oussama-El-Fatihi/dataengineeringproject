# Data Engineering Project

Célia DJOUADI
Oussama EL FATIHI

### Energy Perception vs. Reality in France and Germany

## Table of Contents

1. [Project Presentation](#project-presentation)
   - [Subject](#subject)
   - [Questions We Would Like to Answer](#questions-we-would-like-to-answer)
2. [Data Sources](#data-sources)
   - [Public Opinion Data](#public-opinion-data)
   - [Energy Production and Emission Data](#energy-production-and-emission-data)
3. [Project Steps](#project-steps)
   - [Ingestion Phase (Landing Zone)](#ingestion-phase-landing-zone)
   - [Transformation Phase (Staging Zone)](#transformation-phase-staging-zone)
   - [Production Phase (Curated Zone)](#production-phase-curated-zone)
4. [Analysis and Limitations](#analysis-and-limitations)
5. [Next Steps](#next-steps)
6. [Instructions](#instructions)
   - [Environment Setup](#environment-setup)
   - [Requirements](#requirements)
   - [How to Spin the Project](#how-to-spin-the-project)
     - [Prepping](#prepping)
     - [Running](#running)
     - [Running the Pipeline](#running-the-pipeline)
     - [Visualizations](#visualizations)

---

## Project Presentation

### Subject

The aim of this project is to compare public perception of energy production and emissions with actual data for France and Germany. By analyzing Reddit discussions and official energy datasets, we aim to identify discrepancies between public opinion and the reality of energy production and emissions.

Key aspects include:

1. Sentiment analysis from public discussions related to energy topics on Reddit.
2. Data integration from official sources for energy production and emissions in France and Germany.
3. Data visualization to provide actionable insights.

### Questions We Would Like to Answer

- What are the key differences between public perception and actual data regarding energy production in France and Germany?
- How does sentiment vary across different energy types (e.g., nuclear, renewables)?
- Are public opinions aligned with emission realities?

---

## Data Sources

### Public Opinion Data

- **Source:** Reddit API
- **Description:** Posts and comments extracted from energy-related subreddits in France and Germany.
- **Processing:** Each post and comment was classified for sentiment (positive, neutral, negative) and associated with specific energy topics.

### Energy Production and Emission Data

- **France:**
  - **Source:** [RTE](https://analysesetdonnees.rte-france.com/emission/emission-ges) (manually downloaded CSV).
  - **Data:** Energy production and emissions.
- **Germany:**
  - **Sources:**
    - [Energy-Chart API](https://api.energy-charts.info) (for production data).
    - [Energy-Chart](https://www.energy-charts.info/charts/co2_emissions/chart.htm?l=en&c=DE) (manually downloaded CSV for emission data).

---

## Project Steps

### Ingestion Phase (Landing Zone)

**Objective:** Collect raw data and store it in a transient format.

1. **Public Opinion:**

   - Data from Reddit was ingested using the Reddit API.
   - Specialized subreddits related to energy topics in France and Germany were targeted.
   - Posts and comments were extracted and stored in raw JSON format.

2. **Energy Data:**

   - **France:**
     - CSV files for energy production and emissions were manually downloaded from RTE.
   - **Germany:**
     - Production data was collected through the Energy-Chart API.
     - Emission data was manually downloaded as CSV files.

3. **Storage:** Raw data was stored temporarily in a landing zone, enabling further processing in subsequent phases.

---

### Transformation Phase (Staging Zone)

**Objective:** Clean, wrangle, transform, and enrich the data for analysis.

We use severale libraries for data wrangling such as pandas and also transformer for sentiment analysis

1. **Data Cleaning:**

   - Removed duplicate or inconsistent records.
   - Standardized date formats across datasets for consistency.
   - Handled missing values in energy and emission datasets.

2. **Sentiment Analysis:**

   - Processed Reddit posts and comments using Natural Language Processing (NLP).
   - Classified sentiments into positive, neutral, or negative.
   - Associated each sentiment with relevant energy topics (e.g., nuclear, renewables).

3. **Data Wrangling and Enrichment:**

   - Energy production and emission datasets were normalized and joined.
   - Integrated public sentiment data with energy datasets by energy type and time.

4. **Storage:** Transformed and enriched data was persisted in a durable staging database in Postgres

---

### Production Phase (Curated Zone)

**Objective:** Finalize the data for production analytics and visualization.

1. **Schema Design:**

   The production database is implemented in PostgreSQL using a star schema design. This schema is tailored for efficient querying and analytics, with the following structure:

   - **Fact Table:**
     - **Energy_Impact:**
       This table contains the core metrics and aggregated data for energy production, emissions, and public opinion.
       - Columns:
         - `energy_type_id`: Foreign key referencing `Energy_Type_Dim`.
         - `country_id`: Foreign key referencing `Country_Dim`.
         - `time_id`: Foreign key referencing `Time_Dim`.
         - `source_id`: Foreign key referencing `Source_Dim`.
         - `neutral_opinion`, `negative_opinion`, `positive_opinion`: Sentiment counts.
         - `energy_production`: Total energy production.
         - `energy_emission`: Emissions data.
         - `energy_efficiency`: Calculated efficiency metric.

   - **Dimension Tables:**
     - **Energy_Type_Dim:**
       - Columns:
         - `energy_type_id`: Primary key.
         - `type_name`: Name of the energy type (e.g., nuclear, solar).
         - `renewable_flag`: Boolean indicating if the energy type is renewable.
     - **Country_Dim:**
       - Columns:
         - `country_id`: Primary key.
         - `country_name`: Name of the country (e.g., France, Germany).
     - **Time_Dim:**
       - Columns:
         - `time_id`: Primary key.
         - `month`: Numeric month (1-12).
         - `year`: Four-digit year.
         - Unique constraint: Ensures no duplicate month-year combinations.
     - **Source_Dim:**
       - Columns:
         - `source_id`: Primary key.
         - `source`: Name of the data source (Reddit). 

      **Note:** The `Source_Dim` table has limited utility in this project due to the inability to integrate multiple sources of opinion data. Although production of additional opinion data using LLMs was planned, this was not completed in time. Consequently, only one source (Reddit) is represented, reducing the need for this dimension in the current iteration.

2. **Data Loading:**

   - Transferred data from the staging database into the production database.
   - Apply transformations to add the `energy_efficiency` in Energy_Impact table

3. **Visualization Integration:**

   - Configured Grafana to connect with the production database.
   - Created dashboards to visualize:
     - Energy production vs. public sentiment.
     - Emissions and productions trends over time for France and Germany.

4. **Analytics Queries:**

   - Implemented SQL queries to support dashboard visualizations and answer analytical questions.

---

## Analysis and Limitations

1. **Data Limitations:**
   - **Temporal Gap:** There is a lack of public opinion data, particularly before 2018, as Reddit discussions were more limited or less focused on energy topics in earlier years.
   - **Sampling Bias:** The majority of data scraped from Reddit consisted of comments responding to posts. Comments on Reddit tend to be more negative in tone, leading to a skewed representation of public opinion.

2. **Key Observations:**
   - **Nuclear Energy Sentiment:**
     - In Germany, nuclear energy is predominantly perceived negatively, aligning with the country's policy to phase out nuclear power.
     - In contrast, nuclear energy is viewed more favorably in France, reflecting the country’s reliance on it as a major energy source.
   - **Renewable Energy Sentiment:**
     - Renewables are generally perceived positively in both France and Germany, which is consistent with global trends promoting green energy.

3. **Impact of Limitations:**
   - The sentiment analysis results may not fully represent the broader population’s opinions due to platform-specific biases.
   - While trends can be inferred, a more comprehensive dataset (e.g., surveys, other social media platforms) would enhance the accuracy and breadth of the findings.

---

### Next Steps

1. **Enhance Sentiment Analysis Model:**
   - Improve the existing sentiment analysis model to capture nuanced opinions more accurately, particularly on controversial topics like nuclear energy.

2. **Automate Data Ingestion:**
   - Streamline the ingestion process for manually downloaded datasets, allowing for better scalability and reduced manual effort.

3. **Expand Visualizations:**
   - Develop more detailed dashboards in Grafana, incorporating regional trends within each country for a deeper understanding of localized patterns.

4. **Address Missing Opinion Data:**
   - Leverage advanced Large Language Models (LLMs), such as Gemini, to generate synthetic opinions for missing data points in specific years.
     - Fine-tune the model using historical surveys and statistics to create precise and contextually relevant opinions about energy topics.
     - Use this approach to bridge temporal gaps in public opinion data, particularly for years before 2018.

---

## Instructions

### Requirements

- To have **docker** *and* **docker-compose** installed.
- Install docker and docker-compose exactly as it is described on the official website.
- **Do not use:** `apt install docker` or `apt install docker-compose`.
- **Important:** To run the pipeline in offline mode, please unzip the german_production.json.zip in ./airflow/datasets/ (Due to github size restrictions, we had to zip this dataset)

---

### How to Spin the Project

#### Prepping

- First, get your **id**:

```sh
id -u
```

- Now edit the **.env** file and swap out `501` for your own.

- Run this **once**:

```sh
docker compose up airflow-init
```

- If the exit code is `0`, then it's all good.

- Run the following command to set up the rest of the containers:

```sh
docker compose up --build
```

- You can run the project next time with this command

```sh
docker compose up
```

---

#### Running the project

- Once the server is up, open a new terminal, navigate to the `airflow` directory:

```sh
cd airflow
```

- Execute the following commands **once** to configure the database connections:

```sh
docker compose exec airflow-webserver airflow connections add 'postgres_conn' --conn-uri 'postgres://airflow:airflow@postgres:5432/energy_db'

docker compose exec airflow-webserver airflow connections add 'postgres_conn_staging' --conn-uri 'postgres://airflow:airflow@postgres:5432/staging_db'
```

- After it is up, connect to [localhost:8080](localhost:8080).

---

#### Running the Pipeline

- Once Airflow is running, open your browser and connect to `localhost:8080`.

- Log in using the credentials:

  - **Username:** airflow
  - **Password:** airflow

- Navigate to the "DAGs" tab in the Airflow UI.

- Locate the DAG named `global_dag`.

- Click on the play button (▶) next to the `global_dag` to trigger the pipeline.

- Monitor the execution and logs directly in the Airflow UI to ensure everything runs smoothly.

---

#### Visualizations

- Once the DAG has successfully completed execution, follow these steps to access the visualizations:

  - Open your browser and navigate to [localhost:3000](localhost:3000) to access Grafana.

  - Log in using the following default credentials:

    - **Username:** admin
    - **Password:** admin

  - On the initial setup screen, click on "Skip" to proceed without changing the default password.

  - Navigate to the "Dashboards" section in the Grafana UI.

  - Explore the dashboards to gain insights and validate the results of the pipeline.

--- 
