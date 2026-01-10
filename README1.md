## INGESTION

To simulate a real ingestion, in the same project is the data_retrieval_simulation, which is already uploaded in the github and is ingested in ingestion.py using the raw data

### FILES

**death_causes_province.csv**  
Description:

Comments:


**economic_sector_province.csv**  
Description:

Comments:


**codauto_cpro:**  
Description:

Comments:  
Which will be used for future comparisons to study between Autonomous Communities, Provinces and Municipalities.  
This table has been taken from: https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm and with an AI was put in a CSV. Since this file is very small, it will be included in the staging data field directly.


**pobmun20XX (2008–2024)**

To make things easier, it was changed manually the extension of the archives from an XLS to a CSV, probably there is already a script that does that, because it is not the main focus and it could be done faster by hand, it was made by hand.  
A very good practice could be to be able to determine the type of extension and apply a different ingestion since they work differently, however that is for a much higher level or at least not needed for this project.


## TRANSFORMATION

Spain is administratively divided into Autonomous Communities, which are further divided into Provinces.
Each Province contains several Counties, and each County is composed of multiple Municipalities.

Therefore, each province is composed of every municipalitie of their counties.
This project will be focuses on the Provinces.

Looking at (data/raw/pobmun20XX) the correct way to organice it would be (example):

SPAIN
└──Aragón (Autonomous Community)
    └──Provinces
        ├──Zaragoza
        │   └── Municipalities
        ├──Huesca
        │   └── Municipalities
        └──Teruel
            └── Municipalities

### pobmun20XX.csv

To accomplish the 2–5 CSV mandatories for the project, it is necessary to compact the "pobmun" of each year into a big one. For that operation, we could do it manually, but it is better to apply a transformation from the raw data.

1. There is a useless line in each CSV that starts with "Cifras de poblaci", skipping it solves the issue.

2. Since there is the year in each of the CSVs, a column should be added to identify each municipality in each year, also this decision is made because it will help to match the format with the "death_causes_province" and the "economic_sector_province".

3. Trying to concat the dataframes a problem is found. The columns look like they have the same name, but the total population column (pobXX) is different. Also, the other columns are formatted differently even if they have the same name. Checking that the columns share the same order in the different years:  
"CPRO", "PROVINCIA", "CMUN", "NOMBRE", "POBXX", "HOMBRES", "MUJERES"  
(Finally a good practice coming from the INE), only reformating the name of the columns for each dataset should solve the problem. Since the column year is already added, there will not be a problem to identify the years of the population.  
It has been changed to:  
"CPRO", "PROVINCE", "MUN_NUMBER", "MUN_NAME", "POBLATION", "MALE", "FEMALE", "YEAR"

There is an issue with the 2009 and 2016 datasets with the MUN_NUMBER, it is solved for those in particular.

Also, the correct format is given to prevent future issues.

4. Some of the pobmun datasets have missing values. Checking the fields that are missing, the best practice is different between the columns. There are more than 130,000 rows, and there are missing only a few values, which is very good to do the best study possible with real values.

5. Also, a consistent format is wanted, so the different characters like "," or "()" will be suppressed.

It is possible to see that the values that are missing are the same in number, so most likely the row that is missing one is missing most of them.

Starting easy to hard, the decisions made are:

6. If the MALE or FEMALE column value is missing, just drop the row. The POBLATION row is = MALE + FEMALE, if any of those values is missing that data has no value.

7. If MUN_NAME or MUN_NUMBER is missing, just drop the row. If neither the name nor the number is missing, most likely the data will be useless and the computational cost to impute it having one or the other is not relevant.

8. However, if CPRO or Province name is missing, it is possible to impute it with an auxiliary table without doing too much. This is commented since the developer could not make it work and could not find the issue, but it would be a very good practice.  
So, it is dropped.

After these steps, pobmun_total.csv is ready to be warehoused and studied.

For the other CSVs, the transformation is easier.


### codauto_cpro.csv

9. Just make sure there are no signs like "," or "()" and match the format.


### economic_sector_province.csv

10. An issue with the total is found. The Total column is a string, not a float, so it is necessary to solve that problem.

11. Also, Total Nacional values in the Provincia column are not needed since easily all the provinces can be added.

12. The Province is in one field, and CPRO and CPRO_NAME are two fields, so there is another issue that is resolved with regex.

**KEY STEP**

13. For this project the data is going to be studied per year, not per trimestral, so to solve that issue, first the year and the period need to be in different columns. Since there is a percentage value, the mean should be imputed for each year and the trimestral data would no longer be necessary.

14. For convenience, change the column names and reorder.


### death_causes_province.csv

15. Reformat the Total column to avoid future problems.

16. Also, Nacional values in the Provincia column are not needed since easily all the provinces can be added. Also, we do not want the "Extranjero" (foreigner) data in provinces, since the study is only for Spain residents and it will be an extra problem.

17. Fill the null values that are only in the Total column.

18. Divide the Provincias column into CPRO and CPRO_NAME.

19. Change column names and reorder.

20. FOR ALL the CSVs.  
Checking the files, it is possible to see that the CPROs are formatted differently, so it is necessary to fix that problem.

21. Divide the DEATH_CAUSE column into two different ones since there is a specific code for each name.

Now the transformed CSVs are saved in the staging folder.  
TRANSFORMATION is done.


## WAREHOUSE

After cleaning the data, the next step is to save the data in a warehouse.  
To do this, the developer decided that creating a database with Azure using the Azure Students Subscription is the best option, selecting the scaling option since it can be scaled whenever it is needed. Therefore, the calculations could be more realistic for these points:

- The scalability of the project should be considered, so you need to infer the performance and costs of multiplying the amount of raw data by x10, x100, x1000, x10^6 (that is, the number of rows). A proposal should be made to address those problems.
- How much money will it cost if we migrate this to a cloud provider? Consider the x10, x100, x1000, x10^6 scenarios. Take one cloud provider to calculate it.


### Warehouse Connection

To connect to Azure Services, it is necessary to install the ODBC Driver 18 for SQL Server.  
Search in Google: ODBC Driver 18 for SQL Server Microsoft, go inside learn.microsoft.com.  
In downloads, install the .msi for your PC specs.

Install pyodbc and sqlalchemy in the virtual environment that is being used. This is needed to make Python communicate with Azure SQL.

**Connection Data:**  
user: dwadmin  
password: usj2526.  
server: srv-dw-liliarte-01.database.windows.net  
dw: dw_final_project

Install the "SQL Server (mssql)" extension for Visual Studio Code.

If the connection is not appearing already, just create a new one putting the data above. If this part is not clear to do on the teacher's computer, it will be presented and justified in the oral presentation. Therefore, for now, the changes outside this repo will be added in photos trying to explain it in the best way possible.

Since the teacher has the admin control, he should be able to add his IP to the firewall to use it.

Now it is possible to create the SQL schema. It is created in (data/warehouse/schema.sql). Now facts and dimensions are created in the Azure DW, refresh if it does not appear.

Now the environment is ready to INSERT data with the warehouse.py script. This part is key in the project because now it is possible to automate the INSERTS with Python.


## SQL SCHEMA

The datasets have low complexity, so a Star Schema is the best option, because it has a simple structure, fast query performance, denormalized dimensions, and is easy for business users to understand.

A diagram of the schema is provided in the docs/diagrams folder, this was made in draw.io before the SQL to visualize the schema.

After that, just adjusting the different dimensions and facts in SQL language in schema.sql ends this step.


## WAREHOUSE DATA INSERTION

Once the warehouse schema is created, the next step is to insert the transformed data into Azure SQL.  
This is done using the load_dw.py script.

The script reads the transformed CSV files from the staging folder, cleans and normalizes the data types, and prepares the data to match the warehouse schema.

Dimensions are inserted first, followed by fact tables, to respect foreign key constraints.  
If the CLEAR_BEFORE_LOAD flag is enabled, all tables are emptied before inserting new data, allowing the script to be executed multiple times without conflicts.

For performance reasons, the insertion is done in batches using executemany, which makes the loading process faster and more scalable.

A transaction is used during the load process. If any error occurs, a rollback is applied to avoid leaving the warehouse in an inconsistent state.

After the script finishes, the warehouse is fully populated and ready for analysis.

## MICROSOFT POWER BI
After the correct insertion of the data, is possible to compare and to visualice the evolution in Looker Studio, a key piece of the project.

Looker Studio does not provide a native connector for Azure SQL Database. For this reason, a MySQL database was used as an intermediate analytical layer.

The Data Warehouse is designed and stored in Azure SQL, ensuring scalability and data integrity. Selected fact and dimension tables are replicated into MySQL without modifying the business logic or schema.

MySQL was chosen because it is natively supported by Looker Studio and allows fast and simple integration for visualization purposes.
This approach separates the storage layer (Azure SQL) from the visualization layer (Looker Studio), which is a common pattern in real data architectures.

### MICROSOFT POWER BI Connection

The connection between the Data Warehouse stored in Azure SQL Database and the visualization layer is performed using Power BI Desktop, which provides a native connector for Azure SQL.
Steps to Connect Azure SQL Database to Power BI
Install Power BI Desktop on the local machine.
Open Power BI Desktop and select Get Data from the home screen.
Choose Azure → Azure SQL Database as the data source.

Enter the connection information:
Server name: srv-dw-liliarte-01.database.windows.net
Database name: dw_final_project

Select Import as the data connectivity mode.
Choose Database authentication and enter the SQL credentials:
Username: dwadmin
Password: usj2526.
Click Connect to establish the connection.

Browser-Based Visualization
Once the report is published, it can be accessed directly from the browser using Power BI Service (https://app.powerbi.com), allowing interactive exploration of the data without the need to install Power BI Desktop.

This approach ensures:
Native integration with Azure SQL Database
No intermediate databases or data duplication
A clean separation between the storage layer and the visualization layer
A scalable and professional business intelligence architecture

## ORCHESTRATION

Since in this project great practices were used, and has a great granularity, the orchestration step is very simple to run locally. 
In the orchestration.py file the different steps are in order a succesful execution. 

There are different ways of managing the rollback for each step, because for some processes of the data different of the approaches are better.
Therefore;

ingestion.py: retry (maybe it could not fetch the urls)
transformation.py: stop the pipeline search the problem before restarting
schema.sql: stop the pipeline (maybe a connection error)
load_dw.py: stop the pipeline (duplicate data can cause problems)
main.py: orchestration.py

To run at least once a day

We are not  performing in a production environment, so it feasible to run it once a day in the local machine easily with the scheduler of the PC, however, the request is asking if  the project was made in a production enviorment. Therefore, here is the explanation for 2 different setups:
1. **Docker + Linux (simple and effective)**
2. **Azure-based orchestration (more common for this type of project)**


### Docker + Linux (Simple Production Setup)
A common and simple production solution is to package the entire project inside a **Docker container** and run it on a **Linux server** (either on-premise or in the cloud). This guarantees that the pipeline always runs with the same environment and dependencies.

#### General Idea
The Python project is packaged as a Docker image. The container executes the full pipeline using `main.py`, which calls the orchestration logic. A Linux scheduler (`cron`) runs the container once per day. Logs are stored for later inspection.

#### Why Docker?
Ensures environment consistency (same Python and library versions). Makes local and production executions identical. Simplifies deployment and maintenance.

#### Daily Execution
On the Linux server, a **cron job** is configured to run the Docker container at a fixed time every day (for example, at 02:00 AM).
Each execution:

1. Runs ingestion
2. Applies transformations
3. Loads data into Azure SQL
4. Stops automatically when finished

If any step fails, the container exits and logs can be reviewed.
This approach is sufficient for small and medium-sized pipelines that run once per day.

#### Advantages
* Simple and easy to explain.
* Low operational cost.
* Very close to real industry practice.

#### Limitations
* The server must be maintained manually.
* Scaling and high availability are limited.

### Azure-Based Orchestration (Recommended for This Project)
Since this project already uses **Azure SQL Database** and **Power BI**, running the pipeline directly in Azure is the most natural production approach.

#### Architecture
1. The project is packaged as a Docker image.
2. The image is executed using **Azure Container Apps Jobs** (or a similar Azure service).
3. The job is scheduled to run once per day.
4. Credentials are managed securely using **Azure Key Vault**.
5. Logs and execution status are stored in Azure monitoring services.

#### Secure Credential Management
In a real production environment: Database credentials are **not stored in the code**. Secrets are stored in Azure Key Vault. The container reads them securely at runtime.
This avoids security risks and follows best practices.

#### Scheduling and Monitoring
The job is triggered automatically every day. Azure provides execution logs and status information. Alerts can be configured in case of failure.
This removes the need for a manually managed server and improves reliability.


### Power BI Integration
Once the data is successfully loaded into Azure SQL Database, visualization is handled by **Power BI**.
Two common approaches exist:

#### Scheduled Refresh
Power BI Service refreshes the dataset at a fixed daily tim that is simple and reliable for daily analytics.

#### Pipeline-Triggered Refresh (Advanced)
After a successful pipeline execution, the Power BI dataset refresh is triggered automatically. If the pipeline fails, the dataset is not refreshed. This guarantees data consistency.

#### Conclusions
* Local execution is useful for development and testing.
* In production, automation is mandatory.
* **Docker + Linux** is a simple and effective solution.
* **Azure-based execution** is more robust and better aligned with enterprise environments.
* Both approaches ensure daily execution, controlled failures, and data consistency.



## SCALABILITY
The current implementation of the project processes approximately 130,000 rows of raw data for Spain alone, covering population, mortality, and economic indicators across multiple years and territorial levels. This volume represents a realistic national-scale dataset and serves as a baseline for scalability analysis.

Just taking the biggest dataset since the others have less than 1000 rows so it is not relevant.

If the same data model and pipeline were extended to include additional countries, the number of rows would grow almost linearly with the number of countries and administrative units involved. For example:

x10 data (1.3 million rows)
This scenario corresponds to adding a small group of countries with similar data granularity. At this scale, the current pipeline would remain functional, but ingestion, transformation, and loading times would noticeably increase. Database insert operations and transformation steps would become slower, although still manageable with the existing architecture.

x100 data (13 million rows)
This scale would represent a continental or multi-regional dataset. At this point, performance bottlenecks would emerge, particularly during batch inserts, aggregation operations, and fact table loading. Storage costs and compute usage in the data warehouse would increase significantly, and execution time could become unacceptable without optimization.

x1,000 data (130 million rows)
This scenario approaches a global dataset with detailed historical data. The current approach would no longer scale efficiently: full reloads would be costly, long-running transactions would increase failure risk, and warehouse costs would grow substantially due to storage, I/O, and compute consumption.

x10⁶ data (130 billion rows)
At this extreme scale, the existing architecture would be infeasible. Both performance and cost would become prohibitive, requiring a fundamentally different data processing strategy and infrastructure.

### Performance and Cost Implications

As data volume increases, the main cost and performance drivers are:
ETL execution time, especially during transformation and load stages.
Database storage and indexing costs, which grow with fact table size.
Compute costs in the data warehouse, driven by large batch inserts and analytical queries.
Operational risk, as long-running jobs are more prone to failures and retries.
Without architectural changes, scaling linearly in data volume would result in superlinear increases in cost and processing time.

### Proposed Scalability Improvements
To address these challenges, the following measures are proposed:

#### Incremental Loading
Replace full reloads with incremental loads based on time partitions (e.g., year or month). This ensures that only new or updated data is processed, dramatically reducing execution time and costs.
Partitioning Strategy
Partition large fact tables by time (YEAR) and/or geography (CPRO, country code). This improves query performance and limits the amount of data scanned during analysis.

#### Batch and Parallel Processing
Increase batch sizes and enable parallel ingestion and transformation where possible, especially for multi-country datasets
Separation of Storage and Compute
For very large scales, raw and staging data should be stored in low-cost object storage, while the data warehouse is used only for curated, analytical datasets.

#### Pre-Aggregation and Data Reduction
Introduce aggregated fact tables (e.g., country-year totals) to reduce query cost for common analytical use cases.

#### Cloud Cost Control
Use scalable cloud services with autoscaling and cost monitoring, ensuring that compute resources are only consumed when needed.

# EXECUTION
Just execute the main.py after installing the requierements.txt