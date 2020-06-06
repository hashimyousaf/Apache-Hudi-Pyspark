"""
----------------------------------------------------------------------------------------------------------
Description:

Usage: This method is being used to generate the huge volume data

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
from faker import Faker
from helpermethods import *
from datetime import datetime
from dateutil.relativedelta import relativedelta


faker = Faker()
customers_table_name = 'Customers'
sales_table_name = 'Sales'

def generate_dummy_data(number_of_batches, batch_size, min_range, max_range, table_name, batch, is_json_required=False,
                        is_xml_required=False):
    """
    :param number_of_batches: How many files to be created
    :param batch_size: each file contain the following rows
    :param min_range: random integer should be start from
    :param max_range: random integer should not be greater
    :param table_name: table to be created
    :param is_json_required: For the boolean check to generate json data as well
    :param is_xml_required: For the boolean check to generate xml data as well
    :return:
    """

    header_list = get_table_headers(table_name)
    provider_attributes = get_provider_details(table_name)
    print("----------------------------------\n Data Generation has been started\n")
    print("----------------------------------\n Data Generation is in progress...\n")
    print("----------------------------------\n Following files are being created\n")
    # this iteration create the number of files based on the given number of batches
    for x in range(number_of_batches):
        uuid = faker.uuid4()
        csv_file_name = file_name_provider(table_name + '_' + uuid + '_', "csv")
        # json_file_name = file_name_provider(table_name, "json")
        # xml_file_name = file_name_provider(table_name, "xml")
        """Writing the headers in file name"""
        write_csv_file(csv_file_name, header_list, row_type="header")
        # this iteration runs until to complete the rows in a batch
        year =  generate_random_int(2015, 2020)
        sale_date_year =   datetime.now() + relativedelta(year=year)
        for x in range(batch_size):
            # random variables for generating

            if table_name == customers_table_name:
                customer_id = generate_random_int(min_range, max_range)
                first_name = faker.first_name()
                email = faker.email()
                time_zone = faker.timezone()
                city = get_enum_city_value()
                web_domain = faker.domain_name()
                updated_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                is_deleted = 0

            else:
                random_month =  generate_random_int(1, 12)
                sales_id = generate_random_int(min_range , max_range * 1000)
                customer_id = generate_random_int(min_range, max_range)
                product_name = get_enum_product_value()
                price = generate_random_int(1000, 25000)
                quantity =  generate_random_int(1, 3)
                sale_date = (sale_date_year + relativedelta(month=random_month)).strftime("%d-%m-%Y %H:%M:%S")
                updated_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                is_deleted = 0
            write_csv_file(csv_file_name, eval(provider_attributes))


        # Uploading csv files on S3 buckets
        upload_file_on_s3(csv_file_name, file_type='csv', batch=batch + '/', table_name=table_name.lower() + '/')

        if is_json_required is True:
            write_json_file(csv_file_name, json_file_name)
            # Uploading JSON files on S3 buckets
            upload_file_on_s3(json_file_name, file_type='json')
            # # removing file from local after uploading on S3
            remove_file_from_local(json_file_name)

        if is_xml_required is True:
            write_xml_file(csv_file_name, xml_file_name)
            # Uploading JSON files on S3 buckets
            upload_file_on_s3(xml_file_name, file_type='xml')
            # # removing file from local after uploading on S3
            remove_file_from_local(xml_file_name)

        # removing file from local after uploading on S3
        remove_file_from_local(csv_file_name)
    print("----------------------------------\n Process Completed\n")


if __name__ == '__main__':
    """
     calling the function generate_dummy_data(number_of_batches,batch_size, min_range, max_range, is_json_required, 
     is_xml_required)
    """

    entities = [ customers_table_name, sales_table_name]
    batch = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    for entity in entities:
        generate_dummy_data(1, 3000, 1, 3000, entity, batch, is_json_required=False, is_xml_required=False)
