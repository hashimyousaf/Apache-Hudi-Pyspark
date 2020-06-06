from retrying import retry
import boto3

athena_client = boto3.client('athena')
DATABASE_NAME = "hash_demo"
OUTPUT_LOCATION = 's3://air-product-training-datalake/hashim-yousaf/output-location/'

@retry(stop_max_attempt_number = 10,
    wait_exponential_multiplier = 300,
    wait_exponential_max = 1 * 60 * 1000)
def poll_status(_id):
    result = athena_client.get_query_execution(QueryExecutionId=_id)
    state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception

def run_query(query, database, s3_output):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        })

    QueryExecutionId = response['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))
        print("Here is the actual return of your query : {}".format(result))

if __name__ == '__main__':
    try:
        # createDataBase(ath)
        # createDepartmentsTable(ath)
        with open('sales.sql') as ddl:
            run_query(ddl.read(),DATABASE_NAME, OUTPUT_LOCATION)

        with open('customers.sql') as ddl:
            run_query(ddl.read(),DATABASE_NAME, OUTPUT_LOCATION)

        with open('sales_per_city.sql') as ddl:
            run_query(ddl.read(), DATABASE_NAME, OUTPUT_LOCATION)

    except Exception as exception:
        print("Error ", exception)