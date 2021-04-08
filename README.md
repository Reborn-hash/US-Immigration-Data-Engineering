## Data Engineering Project - US Immigration

#### Scope

This project involved building a data cleaning and ETL process to load local data into AWS S3. The main dataset will include data of over 7 million rows on immigration to the United States, and supplementary datasets will include data on airport codes and U.S. city demographics. The tools used to achieve this are Python and Apache Spark for data processing. Data has been modelled into various tables to be used for analytics derived from the cloud database.

#### Datasets

**I94 Immigration Data:** This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. There's also a sample file available to view in csv format before reading it all in.
<br>**U.S. City Demographic Data:** This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

**Airport Code Table:** This is a table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

### Steps Taken

1. Scope the Project and Gather Data

2. Explore and Assess the Data to identify data quality issues; missing values, duplicate data, etc.

4. Define the Data Model. Map out the conceptual data model and decisions, with steps necessary to pipeline the data into the chosen data model

5. Run ETL to Model the Data
 - Create the data pipelines and the data model
 - Include a data dictionary
 - Run data quality checks to ensure the pipeline ran as expected
    - Integrity constraints on the relational database (e.g., unique key, data type, etc.)
    - Unit tests for the scripts to ensure they are doing the right thing
    - Source/count checks to ensure completeness
<br></br>
6. Complete Project Write Up

#### Workspace Files 
 - etl.py: Executes the file processing and AWS upload mechanisms.
 - README: This projects writeup.
 - DATA: Parquet files written during etl and uploaded to S3
 - dl.cfg: File linking AWS credentials necessary for S3 upload (not included)
 - sas_labels: files containing various mappings obtained from file 'I94_SAS_Labels_Descriptions.SAS' and processed during etl.py.


#### Resulting Data Model
![](Data-Model.png)
<br></br>
These tables allow us to develop analytics which show us various demographics of us immigrants, where they settle, and how long they stay for.

### Decisions Made
- For cleaning and processing of the data, pandas was used where the datasets were small enough, while spark was used to process the immigration data which contains over a million rows as spark provides distributed computing to speed up the process.
- For updating of data, we could use airflow to schedule data pipelined ETL to maintain up to date data in AWS S3. Immigration data should be updated as often as is necessary based on analytics needs. The remaining datasets on states and airport codes could be updated monthly or quarterly depending on how often new state and airport information is posted.

#### Alternative Scenarios
_- If the data was increased by 100x_
<br></br> We could use yarn to manage the scale up of spark clusters to handle the increased workload.
<br></br>
_- If the pipelines were run on a daily basis by 7am_
<br></br> We could use airflow to keep the ETL process on a tight schedule with monitoring in place to ensure that deadlines are met.
<br></br>
_- If the database needed to be accessed by 100+ people._
<br></br> We could maintain the database using Yarn across a number of clusters for high availability, scaling up where needed.
<br></br>
