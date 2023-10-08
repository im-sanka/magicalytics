# Hacktoberfest 2023 project
## Title: Magicalytics!

## Theme:
Extract Transform Load (ETL) pipeline with analytics components.

## Description
This project aims **to provide analytics pipeline which magically turn dataset from different sources into a summary of analytics dashboard with insights**!

The example data is obtained from Google BigQuery and the link can be found [here](https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/google-search-trends).

The tools which will be used are:
- JupySQL
- Poetry
- Google Analytics API
- Google BigQuery API
- will add more ....

*The outcome of this project is an analytical dashboard which can be used to process Google Analytics result and communicate with Retrieval Augmented Generation (RAG) pipeline.*

[//]: # (- Provide a description of your project. Include the data sources you are using, the tools you are using, and the expected outcome of your project.)

## Data sources 

For this project, I use data from Google Analytics data which are tabulated in BigQuery.
However, you may be able to use both single .csv file or any type of database.

## Methods

The methods that I use is shown in the figure below.

Workflow figure will be added here

1. The workflow starts from environment preparation (Conda, Poetry, APIs, etc.).
2. Downloading the data by querying necessary tables from Google BigQuery via API.
3. Splitting the data into different tables based on ranking and deposited as .duckdb database.
4. Building the Exploratory Data Analysis (EDA) pipeline.
5. Making _unique_ analysis using NLP algorithm and other machine learning algorithms.
6. Adding RAG feature to communicate users with analyzed data.
7. Building dashboard for whole visualization and data summary.
8. Deployment via Ploomber.

## User interface
The interface would be a Voila dashboard that host default data from the sample data mentioned above.
The dashboard can also host uploaded data. However, uploaded data will need to be selected first, since the dashboard requires .....

Figure dashboard here...

[//]: # (Describe the user interface your project will have. Include a description of the tools you are using.)

## Team members
For this project, I am the only one who is working on it! :)
Feel free to visit my site as well www.sanka.studio