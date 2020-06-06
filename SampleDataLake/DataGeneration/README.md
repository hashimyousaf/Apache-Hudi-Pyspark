# Data Generator

This utility helps you to generate a large volume of test data in CSV, JSON and XML format based on the given configuration. We can set up the configurations file with the detail relating to the table MetaData in 'config' JSON file by providing the column names and the provider types for each column for a particular table.

## How does it work?

First we need to setup a configuration in *'config'* file for a particular table along with the column details which include the column name and its provider type. Currently there are 40 providers which can be used to generate dummy data against the specified columns in configuration for a table. There is Python file *'data_generator.py'*, in which a function "generate_dummy_data()" is being called in main() by providing some required input parameters. In this utility you can generate multiple batches by specifying the number of rows in each batch. In addition you can place all the generated data into S3 bucket, just by specifying the S3 bucket and the path in config file.

### Required Parameters
function "generate_dummy_data()" takes the following parameters:

```
:param number_of_batches: How many files to be created
:param batch_size: each file contain the following rows
:param min_range: random integer should be start from
:param max_range: random integer should not be greater
:param table_name: table to be created
:param is_json_required: For the boolean check to generate json data as well
:param is_xml_required: For the boolean check to generate xml data as well
```

### List of providers
Following providers can be used to generate dummy data for the particular types of columns.

```
sequence_start
first_name
last_name
full_name
m_name
f_name
phone_number
address
uuid
email
company_email
web_domain
mac_address
web_url
dob
year
month_number
month_name
day_of_week
day_of_month
time_zone
am_pm
datetime_this_century
datetime_this_decade
datetime_this_year
datetime_this_month
date_this_century
date_this_decade
date_this_year
date_this_month
unix_time
datetime_between
date_between
future_datetime
future_date
enum
random_int
random_str_id
random_text
date2
boolean_true_false
boolean_zero_one
```

### Prerequisites
You need to install the following libraries before it's executed.

```
Pip install Boto3
Pip install Faker
Pip install pandas
```

### Other Dependent Libs

```
import random
import string
import csv
from datetime import datetime
import boto3
import os
import Faker
import pandas
```


## How to execute
### To run on local machine
- Clone the repo
- Open project on Pycharm
- Install the dependent libs
- Execute the main() function from "data_generator.py"
### To run on EC2
- Copy this utility on EC2 instance via SCP and S3 Sync Command
- Install dependent libs
- Make sure the EC2 cluster has access across the S3 service
- Execute the following command "Python data_generator.py"

