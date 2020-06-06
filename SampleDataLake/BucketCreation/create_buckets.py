import boto3

def main(main_bucket, developer_name):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=main_bucket, Key=(developer_name + '/'))
    s3.put_object(Bucket=main_bucket, Key=(developer_name + '/landing-data/'))
    s3.put_object(Bucket=main_bucket, Key=(developer_name + '/staging-data/'))
    s3.put_object(Bucket=main_bucket, Key=(developer_name + '/historical-data/'))
    s3.put_object(Bucket=main_bucket, Key=(developer_name + '/curated-data/'))



if __name__ == '__main__':
    main_bucket_name = 'air-product-training-datalake'
    developer_name = 'hashim-yousaf'
    main(main_bucket_name, developer_name)