# currency_exchange_data_ingestor

## Outline

#### 1. **Choose Technology Stack:**

- Python
- pyarrow for handling Parquet files

#### 2. **ETL Architecture:**

- Design a modular architecture with separate components for extraction, transformation, and loading.
- Store data in a folder structure like: year=2023/month=12/day=12
- Use a scheduler (e.g., cron) to automate the ETL process at regular intervals, i.e. daily
	- One job daily data
	- One job for missing dates

#### 3. **Fault Tolerance:**

- Implement retry mechanisms for API calls to handle transient failures.
- Implement backoff mechanism
- Set up logging to capture errors and failures for analysis.
- Backfill data
	- Have a job for a specific date
	- Scan the folder and ingest data for missing dates

#### 4. **Testing:**

- Develop unit tests for each component of the ETL process.
- Implement integration tests to ensure the end-to-end functionality.
- Use mocking for API calls in tests to simulate different scenarios, including API failures.

#### 5. **API Integration:**

- Handle API responses, check for errors, and parse the data.

#### 6. **Data Transformation:**

- Transform the data to the required format (Parquet files) with the necessary columns (date, rate, source currency, target currency).
- Calculate reciprocal rates for every rate to EUR. (to EUR, from EUR)

#### 7. **Data Quality Checks:**

- Implement checks to ensure the quality of the data. For example, check for missing values, outliers, or inconsistencies.
- Log any issues found during the data quality checks.

#### 8. **Parquet File Creation:**

- Use a library like pyarrow or pyspark to create Parquet files.
- Optimize the Parquet file schema and compression settings for efficient storage and retrieval.

#### 9. **Documentation:**

- Document the ETL process, including configuration parameters, dependencies, and steps for troubleshooting.

### Production ready:

To make it production-ready code, consider following:
#### 1. **Monitoring:**

- Set up monitoring for the ETL process, including alerting for failures or issues.
- Use tools like Prometheus, Grafana, or custom logging to monitor the ETL pipeline.

#### 2. **Deployment:**

- Consider containerization (e.g., Docker) for easy deployment and version control.

#### 3. **Suggestions:**

- Use Apache Hive to store data
- Add CI/CD


## Future improvements
- Add fallback URLs: https://github.com/fawazahmed0/currency-api/issues/45
- Better configuration management
- Might not need JSON files really, just transform the data on-the-fly
- Use pyspark if you need to use more transformations to make it easy to scale
- Add more logging
- Make unit tests work 100%
- Add integration tests
- Handle running jobs better
  - Add scheduling
- Add CI/CD
- Add monitoring
- Dockerize the app
