# Dagster ETL Project

## Overview
This project implements an **ETL pipeline** using **Dagster** to process and transform public dataset(s). The pipeline loads raw data, processes it into a structured format, and stores the results for further analysis.

## Getting Started
### Prerequisites
Ensure you have the following installed:
- **Docker** & **Docker Compose**
- **Python 3.10+** (if running locally without Docker)
- **Dagster** and required Python dependencies (specified in `requirements.txt`)

### Installation & Setup
#### Using Docker:
1. Clone this repository:
   ```sh
   git clone https://github.com/your-username/dagster-etl.git
   cd dagster-etl
   ```
2. Build and start the services:
   ```sh
   docker-compose up --build
   ```
3. Access the Dagster UI:
   - Open: [http://localhost:3000](http://localhost:3000)

## Data Used
- **Public Data Source:** The dataset used in this project is publicly available.
- **Data Period:** At least one week's worth of data is included.
- **Data Files:** CSV files are stored in the `dagster_home/data/` directory.

## ETL Pipeline
### Steps in the Pipeline:
1. **Load Data:**
   - Reads raw data from CSV files.
   - Loads data into Pandas DataFrames.
2. **Transform Data:**
   - Cleans, normalizes, and structures the data.
   - Merges datasets where necessary.
3. **Store Processed Data:**
   - Saves processed data in PostgresSQL (Local Storage).

## Data Model
- **List of Orders:** Contains details about orders placed.
- **Order Details:** Holds detailed information about each order.
- **Transformed Data:** A cleaned and merged dataset ready for analytics.

## Enhancements & Future Scope
- Implement cloud storage (e.g., GCS, S3) for better scalability.
- Introduce **data validation** checks before ingestion.
- Automate scheduling using Dagster **sensors & schedules**.

