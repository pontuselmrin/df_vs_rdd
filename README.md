# df_vs_rdd
### Comparing the performance of an aggregate function in RDDs vs that of DataFrames in PySpark

Given the same dataset, different methods can arrive at the same output in different times. In this script, we fetch a small dataset on cost of living from the ECB data warehouse in CSV format and compare a PySpark SQL approach using the built-in SUM function with a PySpark approach using an efficient map transformation. 
