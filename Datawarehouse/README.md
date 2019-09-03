The goal for this project is to setup a data warehouse utilizing the star schema approach to help the startup Sparkify analyze the user usage.  The project will read the log files and song files stored in S3 and import them into our staging tables first before ETL-ed into the dimension tables and fact table.

To run the project, open up your terminal window and run create_tables.py first as it will delete the tables from our redshift cluster and recreate the table.  Once the table's created, you can run the etl.py to import data from flat files stored in S3 and into our staging tables first.  Then it will take the data from staging table and import them into the tables under the star schema.

This project consists of the following files:

1. create_tables.py -> This python file contains the logic to delete and recreate the tables.
2. dwh.cfg -> This is the config file the stores the setting for the project.
3. etl.py -> Tbis python file contains the logic to copy csv files into the staging data and from staging data into our tables under the star schema.
4. sql_queries.py -> This file contains the create table statements and insert data logic.
5. cluster_management.ipynb -> This is a jupyter notebook that contains the logic to create and delete the redshift cluster.
