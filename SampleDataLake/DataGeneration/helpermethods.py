"""
----------------------------------------------------------------------------------------------------------
Description:

Usage: Helper methods to accommodate the data generator method

Author  : Usman Zahid
Release : 1
Modification Log:

This can be executed on local or as well as on any EC2 instance
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
30/03/2020        Usman Zahid        			            Initial draft.

-----------------------------------------------------------------------------------------------------------
"""

from jsonparser import JsonParser
from datetime import datetime
from faker import Faker
import boto3
import os
import random
import string
import csv
import pandas as pd


def file_name_provider(table_name, file_type='csv'):
    """
    this function returns the file name with current timestamp
    :param table_name: table name to be used with timestamp
    :param file_type: file type
    :return: File Name
    """
    filename1 = datetime.now().strftime("%Y%m%d-%H%M%S")
    local_path = "C:/Users/"
    if file_type is 'csv':
        file_name = str(table_name + str(filename1) + ".csv")
        print(file_name)
        return file_name
    elif file_type is 'json':
        file_name = str(table_name + str(filename1) + ".json")
        print(file_name)
        return file_name
    elif file_type is 'xml':
        file_name = str(table_name + str(filename1) + ".xml")
        print(file_name)
        return file_name
    else:
        print("File name cannot be found")


def write_csv_file(file_name, row_to_save, row_type=''):
    """
    This method is being used to write the csv files
    :param file_name: Requires a file name, where the data will be appended
    :param row_to_save: Record's row to be appended in CSV file
    :param row_type: Catering to add headers in CSV file
    :return: True
    """
    if row_type is "header":
        with open(str(file_name), "w") as csvFile:
            writer = csv.writer(csvFile, delimiter=',')
            writer.writerows([row_to_save])
            csvFile.close()
            return True
    else:
        with open(str(file_name), 'a') as csvFile:
            writer = csv.writer(csvFile, delimiter=',')
            writer.writerow(row_to_save)
            csvFile.close()
            return True


def generate_random_int(min_range, max_range):
    """
    This method is being used to generate random integers
    :param min_range: random integer should be start from
    :param max_range: random integer should not be greater
    :return: random integer
    """
    rand_int = (random.randint(min_range, max_range))
    return rand_int


def random_string_id(string_length=6):
    """Generate a random string of fixed length """
    letters = string.ascii_letters
    rand_string = ''.join(random.choice(letters) for i in range(string_length))
    random_string = rand_string+"-"+rand_string+"-"+rand_string
    return random_string


def get_boolean_true_false():
    """
    This function is being used to generate boolean data
    :return: returns boolean value either it is true or False
    """
    faker = Faker()
    b = faker.words(1, ['True', 'False'], True)
    return b[0]


def get_boolean_zero_one():
    """
    This function is being used to generate boolean data
    :return: returns boolean value either it is 0 or 1
    """
    faker = Faker()
    b = faker.words(1, [0, 1], True)
    return b[0]


def get_enum_value():
    """
    This method is being used to get enum value
    :return: enum value
    """
    faker = Faker()
    enum = faker.words(1, ['small', 'medium', 'large', 'extra large'], True)
    return enum[0]

def get_enum_city_value():
    """
    This method is being used to get enum value
    :return: enum value
    """
    faker = Faker()
    enum = faker.words(1, ['Lahore', 'Islamabad', 'Gujrat', 'Multan', 'Karachi', 'Faisalabad', 'Sialkot', 'Gujranwala'], True)
    return enum[0]

def get_enum_product_value():
    """
    This method is being used to get enum value
    :return: enum value
    """
    faker = Faker()
    enum = faker.words(1, ['Keyboard', 'Laptop', 'Car',
                           'Motorcycle', 'Cold Drink', 'Football',
                           'Mobile', 'Ice Cream', 'Furniture',
                           'Air Conditioner', 'Television', 'Property'], True)
    return enum[0]


def upload_file_on_s3(file, file_type='', batch='', table_name=''):
    """
    Uploading the files on S3 buckets
    :param file: CSV file to be placed on S3
    :param file_type: get the path based on file type
    :return: None
    """
    s3_detail = JsonParser().read_json("bucket_path")
    s3 = boto3.resource('s3')
    if file_type is "csv":
        s3.meta.client.upload_file(file,
                                   s3_detail[0], s3_detail[1]+batch+table_name+file)
    elif file_type is "json":
        s3.meta.client.upload_file(file,
                                   s3_detail[0], s3_detail[2] + file)
    elif file_type is "xml":
        s3.meta.client.upload_file(file,
                                   s3_detail[0], s3_detail[3] + file)
    else:
        print("File cannot be uploaded")


def remove_file_from_local(table_filename):
    """
    This method will remove the file from local
    :param table_filename: File to be removed from local directory
    :return: None
    """
    os.remove(table_filename)


def get_table_headers(table_name):
    """
    This method is being used to get the header details from JSON config file for a given table
    :param table_name: Table name to get metadata from config file
    :return: headers
    """
    json_config_obj = JsonParser()
    config_list = json_config_obj.read_json(table_name)
    header_list = []
    for i in range(0, len(config_list), 2):
        attr_1 = config_list[i]
        header_list.append(attr_1)
    headers = ','.join(header_list)
    return header_list


def get_provider_details(table_name):
    """
    This method is being used to get the provider details for each column from JSON config file for a given table
    :param table_name: Table name to get metadata from config file
    :return: providers
    """
    json_config_obj = JsonParser()
    config_list = json_config_obj.read_json(table_name)
    provider_list = []
    for i in range(0, len(config_list), 2):
        list_attr = config_list[i + 1]
        provider_list.append(list_attr)
    providers = ','.join(provider_list)
    return providers


def func(row):
    """
    This function is setting a tree of xml based on the given data
    :param row:
    :return:
    """
    xml = ['<item>']
    for field in row.index:
        xml.append('  <field name="{0}">{1}</field>'.format(field, row[field]))
    xml.append('</item>')
    return '\n'.join(xml)


def write_xml_file(csv_file_name, xml_file_name):
    """
    Storing the data in xml file
    :param csv_file_name: file from the data is being read
    :param xml_file_name: file where the data is being stored
    :return: None
    """
    df = pd.read_csv(csv_file_name, sep=';')
    xml = '\n'.join(df.apply(func, axis=1))
    with open(str(xml_file_name), "w") as f:
        f.write(str(xml))
    return None


def write_json_file(csv_file_name, json_file_name):
    """
        Storing the data in json file
        :param csv_file_name: file from the data is being read
        :param json_file_name: file where the data is being stored
        :return: None
        """
    df = pd.read_csv(csv_file_name, sep=';')
    df.to_json(json_file_name, orient="records")
    return None

