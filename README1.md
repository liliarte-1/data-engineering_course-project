INGESTION:
To make things easier, It was changed manually the extension of the archives from an xls to a csv, probably there is already an scrip that does that, because is not the main focus and it could be done faster by hand, it was made by hand. 
A very great practice could be to be able to determine the type of extension and apply a different ingestion since they work different, however that is for a much higher level or at least not needed for this project.

TRANSFORMATION

To acomplish the 2-5 csv mandatories for the project, is necessary to compact the "pobmun" of each year into a big one. For that operation, we could do it manually, but is better to apply a transformation from the raw data.


1. There is a useless line in each csv that starts with "Cifras de poblaci", skipping it solves the issue.

2. Since there is the year in each of the csvs, a column should be added to identify each municipality in each year, also this decision is made because it will help to match the format with the "death_causes_province" and the "economic_sector_province"

3. Trying to concat the dataframes a problem is found. The columns look like they have the same name, but the total poblation column (pobXX) is different. Also, the other columns are formated different even if they have the same name. Checking that the columns share the same order in the different years:
"CPRO", "PROVINCIA", "CMUN", "NOMBRE", "POBXX", "HOMBRES", "MUJERES"
 (Finally a good practice coming from the INE), only reformating the name of the columns for each dataset should solve the problem. Since the column year is already added, there will not be a problem to identify the years of the population.
It is been changed to:
"CPRO", "PROVINCE", "MUN_NUMBER", "MUN_NAME", "POBLATION", "MALE", "FEMALE", "YEAR"

4. Some of the Pobmun datasets have missing values. Checking the fields that are missing areThe best practice to solve this is to impute the media  
