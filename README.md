# data-engineering_course-project
This repo has the USJ 2025-2026 Data Engineering Course Project made by Javier Liarte

TRANSFORMATION:
Spain is administratively divided into Autonomous Communities, which are further divided into Provinces.
Each Province contains several Counties, and each County is composed of multiple Municipalities.

Therefore, each province is composed of every municipalitie of their counties.
This project will be focuses on the Provinces.
Also, there is whole Spain data, which we will keep it because it might be interesting to study in the future.

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
 
This is used to verify the data and to agrupate it. 
Zaragoza: 50001 – 50999
Huesca: 22001 – 22999
Teruel: 44001 – 44999


It is important to save the raw data to compare the before and after to keep track of how many values were correct or had changed.

1. First, the name and the postal code are in the same column, so is compulsory to divide it.
    After, the ["Lugar de residencia"] column is not necesary




