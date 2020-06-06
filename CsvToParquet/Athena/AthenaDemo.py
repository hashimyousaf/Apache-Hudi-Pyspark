import boto3

def createDataBase(athena_client):
    athena_client.start_query_execution(
        QueryString='create database if not exists  hash_demo',
        ResultConfiguration={'OutputLocation': 's3://testhasbucket/boto3target/'})

def createDepartmentsTable(athena_client):
    with open('create_departments.sql') as ddl:
        athena_client.start_query_execution(
            QueryString=ddl.read(),
            ResultConfiguration={'OutputLocation': 's3://testhasbucket/boto3target/'})

def convert_departments_into_parquet(athena_client):
    with open('departments_parquet.sql') as ddl:
        athena_client.start_query_execution(
            QueryString=ddl.read(),
            ResultConfiguration={'OutputLocation': 's3://testhasbucket/boto3target/'})

if __name__ == '__main__':
    try:
        ath = boto3.client('athena')
        # createDataBase(ath)
        # createDepartmentsTable(ath)
        convert_departments_into_parquet(ath)
    except Exception as exception:
        print("Error ", exception)