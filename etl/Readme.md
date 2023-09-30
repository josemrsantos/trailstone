# Trailestone - ETL framework to get data from API

## Problem statement
**Write an ETL client that will**:  
 1. Extract data only from latest week from both: Solar and Wind endpoints.
 2. Transform naive timestamps from the data source to a time zone aware UTC format.
 3. Ensure column naming and column type are adhering proper naming strategy and types.
 4. Finally load the data to an /output directory, using a file format of your choosing. Keeping in mind directory and filename structure/convention.

## Overview of the solution
### Approach to the problem
If we are not careful, sometimes we can spend too much time on a new feature and deliver more than what really is
needed. Other times one can rush the delivery and not deliver enough of what is required. For this exercise (and as a 
thumb of rule for everything I do in life), I have tried going for the best of both worlds: Deliver what was requested 
in a sensible amount of time. I believe this can be considered an MVP as a fair amount has been made, but there is still
 much room for improvements.
### Architecture
I created 3 Python modules for this solution: extract, transform, and load. I have also created a Python script (etl.py)
 that should put all together, from receiving options as parameters to having logic to call the data flow for each day.

The overview of the architecture is to input a date (or leave that blank and yesterday will be assumed). 

The reason to choose Yesterday (and not Today) as the base date is because we might want to execute this data flow at 
any time, and I assume that the production API will be returning **historic** data (not **future predictions**). When 
calling the local API, it returns data for any date (in the past or future) that we ask. I am assuming that the 
production API would not return any values for events that have not happened yet, therefore it is safer to get 7 days 
from Yesterday and not from Today.

All the code described bellow has been designed in a way that promotes **reusalability**, **extensability**, 
**testability** and **readability**.
## Extract
This Python module is in the file **extract.py** and its goal is to extract the data (as is) from one of the APIs.

The main class in this module is **APIExtractor**. Besides fetching the data from an API as already mentioned, it also 
performs some cleansing of the column name: Removes trailing and leading spaces. Replaces remaining spaces with 
underscores and lowercase's all letters in it.
## Transform
This Python module is in the file **transform.py** and its goal is to transform the data obtained from the previous 
module.

The main class in this module is **APITransformer**.For now, and as it was the only requested transformation, this only 
handles:
 * Transform naive timestamps from the data source to a time zone aware UTC format

## Load
This Python module is in the file **load.py** and its goal is to output the data obtained from the previous 
module, into files in the file system.

The main class in this module is **APILoader**. The file format that I decided to use was parquet. This could help 
query the data, if instead of simply dumping into a file on the local filesystem, the files (and the directory 
structure) were being transferred into S3, and a hive table was added to a hive catalogue. It would not be too 
difficult to change this module to have the output done to S3.

## Full ETL script

The main script is **etl.py** and that script can be called by running:
```commandline
python3 etl.py --key APY_KEY\!\?\@ --today 2023-09-30 --days_back 7
```
Note that **today** is optional (it will default to the actual day the script is run) and so is **days_back** (it will 
default to 7).  
For the previous run the following file structure would be created:
```commandline
output/
├── solar
│   ├── 2023-09-23
│   │   └── data.parquet
│   ├── 2023-09-24
│   │   └── data.parquet
│   ├── 2023-09-25
│   │   └── data.parquet
│   ├── 2023-09-26
│   │   └── data.parquet
│   ├── 2023-09-27
│   │   └── data.parquet
│   ├── 2023-09-28
│   │   └── data.parquet
│   └── 2023-09-29
│       └── data.parquet
└── wind
    ├── 2023-09-23
    │   └── data.parquet
    ├── 2023-09-24
    │   └── data.parquet
    ├── 2023-09-25
    │   └── data.parquet
    ├── 2023-09-26
    │   └── data.parquet
    ├── 2023-09-27
    │   └── data.parquet
    ├── 2023-09-28
    │   └── data.parquet
    └── 2023-09-29
        └── data.parquet
```
It is also important to note that running the same request (defining today and days_back) multiple times should always 
create the same output

### Future considerations
I have delivered here a possible solution for what was requested.  
But looking a bit more indepth other things should be considered such as:
 - **More logs** that show how the data was in every stage (for debugging in case something fails).
 - **Sanity checks**. This might be needed either at the end of the entire dataflow, for a given day or even for each 
step (depending on defined SLAs).
