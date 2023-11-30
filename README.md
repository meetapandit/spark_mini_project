# Spark Mini Project to find Post Car sales analysis of accident records
-  Steps to run the data_upload.py file:
-  Upload the raw data to any storage platform like hdfs, Google cloud storage or s3
-  Define a schema for the raw data before reading into spark for transformation
-  Propogate make, model and year vales to other incident_type records like incident_type = A (accident) and incident_type = R (repair)
  -  Use window function to group the data by vin_number and order by the corresponding column (make, model an year)
  -  Use the Window function to get the last non-null value for each column to populate all blank records for A and R type records
- Groupby and aggregate by make and year to get the count of accidents

 ### Schema for reading file from cloud storage
 
![Screenshot 2023-11-30 at 11 18 51 AM](https://github.com/meetapandit/spark_mini_project/assets/15186489/b9adab61-37c4-4b14-8988-283663e44a1d)

 ### DataFrame after propogating make, model and year values to all incident_type records

![Screenshot 2023-11-30 at 11 18 51 AM](https://github.com/meetapandit/spark_mini_project/assets/15186489/c66a65a1-cdfc-47b0-a8e9-929164da4b89)

 ### Count of accident records by make and year

![Screenshot 2023-11-30 at 11 20 25 AM](https://github.com/meetapandit/spark_mini_project/assets/15186489/f99d3c41-0787-40d0-b6f6-833637664274)
