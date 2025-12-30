# data-engineering_course-project
This repo has the USJ 2025-2026 Data Engineering Course Project made by Javier Liarte

TRANSFORMATION:
Spain is administratively divided into Autonomous Communities, which are further divided into Provinces.
Each Province contains several Counties, and each County is composed of multiple Municipalities.

Therefore, each province is composed of every municipalitie of their counties.
This project will be focuses on the Provinces.

Looking at (data/raw/0201010101.csv) the correct way to organice it would be:
SPAIN
└──Aragón (Autonomous Community)
    └──Provinces
        ├──Zaragoza
        │   └── Municipalities
        ├──Huesca
        │   └── Municipalities
        └──Teruel
            └── Municipalities


In Spain, postal codes are province-based, meaning that the first two digits identify the province. This allows municipalities to be associated with provinces using the following postal code ranges:
 
1. This is used to verify the data and to agrupate it. 
Zaragoza: 50001 – 50999
Huesca: 22001 – 22999
Teruel: 44001 – 44999

Since the postal code is unique, it will be used as the primary key.

It is important to save the raw data to compare the before and after to keep track of how many values were correct or had changed.

2. The name and the postal code are in the same column, so is compulsory to divide it.
    After, the ["Lugar de residencia"] column is not necesary.
    Also, is necessary to drop the MUNICIPIOS line, which is useless and is only to separate from Spain.

3. Looking at the csv, all the data of the columns has been taken in first of January, so it is not necessary to mantain the day, is better just to mantain to year.

4. To manage the missing values, the best metric in population is to impute the media. But, the media that has to be imputed is the media of the municipality, not the global media.

5. IMPORTANT: 
The smartest way to study the new data and to be more efficiente and easy to sort, is to reformat it to share the format of the (data_retrieval_simulation/households_dataset.csv), so we will add more rows to remove more columns. This reduces complexity in computation and is key in the project. This has been made after managing the missing values because at least for the developer was easier, however, is better to make this change at the start.

6. After cleaning, now is possible to get the Provinces data, and save it in a different df.

7. Now is possible to create the provinces data for each year. 

8. A merge is used to make this possible. Note: The merge is between province code.

9.  province_code is dropped because is no longer necessary and is possible to reobtain it easily.

10. Sorting the columns will make it easier for next steps.

11. Also, there is whole Spain data, which we will keep it in a separate csv, because it might be interesting to study in the future.

These steps were made for (data/raw/0201010101.csv). Since the (data/raw/households_dataset.csv) is almost clean, is only necessary to apply a few of the steps above

12. households_dataset transformation. Luckily, is not necessary to impute data since there is not any missing value. IMPORTANT: renaming the variables should be necessary to pivot, luckily in this case is not necessary

WAREHOUSE
For the warehouse, an OLAP (Online Analytical Processing) model will be implemented, because the goal is to study the business intelligence and analytics. Therefore, denormalized data will be used. The OLTP will not be covered, since in this project there is no online transaction processing and is not necessary to cover the requirements. However, in a real bussiness project, the OLTP model is crucial too, and both models are related even if the goal of each one is different.


The datasets have low complexity, so and Star Schema is the best option, because it has simple structure​, fast query performance​, denormalized dimensions​, easy for business users to understand​

To connect to Azure Services, is necessary to install the ODBC Driver 18 for SQL Server
Search in google: ODBC Driver 18 for SQL Server Microsoft go inside learn.microsoft.com...
In downloads, install the .msi for your pc specs.

Install pyodbc and sqlalchemy in the virtual environment that is being used. This is needed to make Python communicate with Azure SQL

Conection Data:
user: dwadmin
password: usj2526.
server: srv-dw-liliarte-01.database.windows.net
dw: dw_final_project

Install the "SQL Server (mssql)" extension for Visual Studio Code.

If the connection is not appearing already, just create a new one putting the data above. If this part is not clear to do on the teacher's computer, it will be presented and justified in the oral presentation. Therefore, since now, the changes outside this repo will be added in photos trying to explain in in the best way possible.

Now it is possible to create the SQL Schema. It is created in (data/warehouse/schema.sql). Now facts and dimensions are created in the Azure DW, refresh if it does not appear.

Now the environment is ready to INSERT data with the warehouse.py script. This part is key in the project because now is possible to autommatice the INSERTS with Python.

 




