INGESTION:

FILES: 
death_causes_province.csv
Description:

Coments:


economic_sector_province.csv
Description:

Coments:


codauto_cpro:
Description:

Coments:
Which will be used for future comparisons to study between Autonumous Comunnities, Provinces and Municipalities.
This table has been taken from: https://www.ine.es/daco/daco42/codmun/cod_ccaa_provincia.htm and with an AI was put in a csv. Since this file is very small will be included in the staging data field directly.

pobmun20XX (2008-2024)

To make things easier, It was changed manually the extension of the archives from an xls to a csv, probably there is already an scrip that does that, because is not the main focus and it could be done faster by hand, it was made by hand. 
A very great practice could be to be able to determine the type of extension and apply a different ingestion since they work different, however that is for a much higher level or at least not needed for this project.



TRANSFORMATION

pobmun20XX.csv:
To acomplish the 2-5 csv mandatories for the project, is necessary to compact the "pobmun" of each year into a big one. For that operation, we could do it manually, but is better to apply a transformation from the raw data.

1. There is a useless line in each csv that starts with "Cifras de poblaci", skipping it solves the issue.

2. Since there is the year in each of the csvs, a column should be added to identify each municipality in each year, also this decision is made because it will help to match the format with the "death_causes_province" and the "economic_sector_province"

3. Trying to concat the dataframes a problem is found. The columns look like they have the same name, but the total poblation column (pobXX) is different. Also, the other columns are formated different even if they have the same name. Checking that the columns share the same order in the different years:
"CPRO", "PROVINCIA", "CMUN", "NOMBRE", "POBXX", "HOMBRES", "MUJERES"
 (Finally a good practice coming from the INE), only reformating the name of the columns for each dataset should solve the problem. Since the column year is already added, there will not be a problem to identify the years of the population.
It is been changed to:
"CPRO", "PROVINCE", "MUN_NUMBER", "MUN_NAME", "POBLATION", "MALE", "FEMALE", "YEAR"
Also, the correct format is given to prevent future issues

4. Some of the Pobmun datasets have missing values. Checking the fields that are missing, the best practice is different between the columns. There are more than 130000 rows, and there are missing only a few values, which is super good to do the best study possible with real values.

5. Also, a consistent format is wanted, so the different characters like "," will be supressed

It is possible to see that the values that are missing are the same in number, so most likely the row that is missing one is missing most of them.

Starting easy to hard the decisions made are:

6. If the MALE or FEMALE column value is missing just drop the row. The POBLATION row is = MALE + FEMALE, if any of that values is missing that data has no value.

7. If MUN_NAME or MUN_NUMBER is missing just drop the row. If the name nor the number is missing most likely the data will be useless and the computational cost to impute it having one or other is not relevant.

8. However, if CPRO or Province name is missing is possible to impute it with an auxiliar table without doing to much. This is commented since the developer could not make it work and he could not find the issue, but it will be a very good practice
So, we drop it. 

After that steps, pobmun_total.csv is ready to be warehoused and studied.

For the other csvs, the transformation is easier. 

codauto_cpro.csv:
9. Just make sure there is no signs like "," and match the format.


economic_sector_province.csv:
10. A issue with the total is found. The Total column is a string, not a float, so is necessary to solve that problem.

11. Also, Total Nacional values in Provincia column are not needed since easily all the provinces can be added.

12. The Province is in 1 fields, and CPRO and CPRO_NAME are 2 fields, so there is the other issue that is resolved with regex.

KEY STEP
13. For this project the data is going to be studied per year, not per Trimestral, so to solve that issue, first the year and the period need to be in different cols. Since there is percentage value, the media should be imputed for each year and the trimestral data would no longer be necessary. 

14. For convinience change the column names and reorder


death_causes_province.csv:


Now the transformed csvs are saved in the staging folder

