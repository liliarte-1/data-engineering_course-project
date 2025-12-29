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
The smartest way to study the new data and to be more efficiente and easy to sort, is to reformat it to share the format of the (data_retrieval_simulation/households_dataset.csv), so we will add more rows to remove more columns. This reduces complexity in computation and is key in the project. This has been made after managing the missing values 
because at least for the developer was easier, however, is better to make this change at the start.

6. After cleaning, now is possible to get the Provinces data, and save it in a different df.

7. Now is possible to create the provinces data for each year. 

8. A merge is used to make this possible. Note: The merge is between province code.

9.  province_code is dropped because is no longer necessary and is possible to reobtain it easily.

10. Sorting the columns will make it easier for next steps.

11. Also, there is whole Spain data, which we will keep it in a separate csv, because it might be interesting to study in the future.

These steps were made for (data/raw/0201010101.csv). Since the (data/raw/households_dataset.csv) is almost clean, is only necessary to apply a few of the steps above

12. households_dataset transformation. Luckily, is not necessary to impute data since there is not any missing value. IMPORTANT: renaming the variables should be necessary to pivot, luckily in this case is not necessary




 




