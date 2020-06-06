# Apache-Hudi-Pyspark
Anyone who wants to implement data lake using Apache hudi can get sample code from this repo.
Here are the few useful links of the Constant Parameters of Apache Hudi used in the code snippets:
  1. https://hudi.apache.org/docs/configurations.html
  2. https://github.com/apache/hudi/blob/master/hudi-spark/src/main/scala/org/apache/hudi/DataSourceOptions.scala

As we all know actualy implementation of the solution is in Scala. But when we go to implement it using python, you need to provide values of some constants. That's why I have pasted the second link, you can get all values of your constants from there.

### Tools and technologies involved
  1. AWS EMR
  2. AWS Athena
  3. Apache Spark
  4. AWS S3
  
### Data Generation
You can also set up data generation to mock the data pipeline flow. But you have to set up this data generator in your local machine or you can also set up any Ec2 instance. Data generator will put data into your raw data buckets and one can also setup cron schedule to run this data generation without any manual intervention. 
#### Path of Data Generator : https://github.com/hashimyousaf/Apache-Hudi-Pyspark/blob/master/SampleDataLake/DataGeneration/
  
### Apache-Hudi on AWS EMR
And if you are running it on AWS EMR, there is also some jars uploaded in the repo, you can use to set it up and make it runable on AWS EMR.



