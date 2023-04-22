import pymysql
import sys
import boto3
import os
from datetime import datetime
import psycopg2
from pathlib import Path
from typing import List
import shutil


def create_folder(folder_path: str, clear_folder: bool=False):
    """
    Create a folder, optionally clear the folder
    """
    path = Path(folder_path)
    if not path.is_absolute():
        path = Path().joinpath(folder_path)
    
    if clear_folder and path.exists() and path.is_dir():
        shutil.rmtree(path.as_posix());
    
    path.mkdir(parents=True, exist_ok=True)


def select_rds_into_s3(rds_host, user, passwd, db_name, query, s3_path, port = 3306)->bool:
    """
    Export data from RDS to a S3 Bucket.
    """
    try:
        conn =  pymysql.connect(host=rds_host, user=user, passwd=passwd, port=port, database=db_name)
        cur = conn.cursor()
        cur.execute(f"{query} into OUTFILE S3 '{s3_path}' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';")
        query_results = cur.fetchall()
        print('Exported RDS data into s3 bucket')
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print("Failed to export RDS data into s3 bucket. {}".format(e)) 
        return False


def unload_redshift_to_s3(db_host, db_name, user, password, query, s3_path, iam_role, aws_region, port='5439')->bool:
    """
    Unload data from Redshift to a S3 bucket.
    """
    try:
        con=psycopg2.connect(host=db_host, dbname=db_name, port=port, user=user, password=password)
        #Unload Command as Variable
        unload_command = f"unload ('{query}') to '{s3_path}' credentials 'aws_iam_role={iam_role}' delimiter ',' region '{aws_region}';"
        
        #Opening a cursor and run unload query
        cur = con.cursor()
        cur.execute(unload_command)
        
        print('Exported Redshift data into s3 bucket')
        #Close the cursor and the connection
        cur.close()
        con.close()
        return True
    except Exception as e:
        print("Failed to export Redshift data into s3 bucket. {}".format(e)) 
        return False


def list_s3_files_by_prefix(bucket_name: str, prefix: str, s3_client=None):
    """
    Return list of files in the bucket which matches the prefix
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = response.get("Contents")
    return files if files else []


def download_s3_files(bucket_name: str, file_keys: List[str], subfolder_path: str, s3_client=None) -> List[str]:
    """
    Download a list of files from bucket
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    
    downloaded_files = []
    cur_dir = Path()
    folder_path = cur_dir.joinpath(subfolder_path)
    folder_path.mkdir(parents=True, exist_ok=True)
    for key in file_keys:
        print('Downloading file ', key)
        file_path = folder_path.joinpath(key)
        print(file_path.absolute())
        s3_client.download_file(
            Bucket=bucket_name, Key=key, Filename=file_path.as_posix()
        )
        downloaded_files.append(file_path)
        
    return downloaded_files


datetime_str = datetime.now().strftime("%Y%m%d_%H%M%S")

REGION="ap-southeast-1"
S3_BUCKET = 'temp-460453255610'

# Export data from RDS to S3 Bucket
RDS_ENDPOINT="database-3.cluster-c3bottoj4h9o.ap-southeast-1.rds.amazonaws.com"
RDS_DB="stackoverflow"
RDS_USER="admin"
RDS_PASSWD = "Qwer!234"
RDS_PREFIX = f'rds_{datetime_str}'
RDS_QUERY = 'select id from posts'
s3_path_rds = f's3-{REGION}://{S3_BUCKET}/{RDS_PREFIX}'

# Export data from Redshift to S3 Bucket
REDSHIFT_HOST = 'redshift-cluster-1.cs21sivqdtax.ap-southeast-1.redshift.amazonaws.com'
REDSHIFT_DB = 'dev'
REDSHIFT_USER = 'awsuser'
REDSHIFT_PASSWD = 'Qwer!234'
REDSHIFT_PREFIX = f'redshift_{datetime_str}'
REDSHIFT_QUERY = 'select id from stackoverflow.posts'
REDSHIFT_IAM_ROLE = 'arn:aws:iam::460453255610:role/RedshiftImportFromS3'
s3_path_redshift = f's3://{S3_BUCKET}/{REDSHIFT_PREFIX}'


print('Select from RDS into S3...')
select_rds_into_s3(RDS_ENDPOINT, RDS_USER, RDS_PASSWD, RDS_DB, RDS_QUERY, s3_path_rds)

print('Unload from Redshift into S3...')
unload_redshift_to_s3(REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWD, REDSHIFT_QUERY, s3_path_redshift, REDSHIFT_IAM_ROLE, REGION)


# Find matching files in S3 and download them
files = list_s3_files_by_prefix(bucket_name=S3_BUCKET, prefix=RDS_PREFIX)
create_folder('data_rds', True)
print('Download RDS data from S3...')
rds_files = download_s3_files(S3_BUCKET, [file['Key'] for file in files], 'data_rds')

rds_data = []
for file in rds_files:
    with open(file) as f:
        rds_data.extend(f.readlines())

# Find matching files in S3 and download them
files = list_s3_files_by_prefix(bucket_name=S3_BUCKET, prefix=REDSHIFT_PREFIX)
create_folder('data_redshift', True)
print('Download Redshift data from S3...')
redshift_files = download_s3_files(S3_BUCKET, [file['Key'] for file in files], 'data_redshift')

redshift_data = []
for file in redshift_files:
    with open(file) as f:
        redshift_data.extend(f.readlines())


print(len(rds_data), len(redshift_data))

rds_data.append('DUMMY in RDS')
redshift_data.append('DUMMY in REDSHIFT')

only_in_rds = set(rds_data).difference(set(redshift_data))
print('Only in RDS:', len(only_in_rds))

only_in_redshift = set(redshift_data).difference(set(rds_data))
print('Only in Redshift:', len(only_in_redshift))


if only_in_rds or only_in_redshift:
    print('Only in RDS:', only_in_rds)
    print('Only in Redshift:', only_in_redshift)
    raise Exception('Data is different between RDS and Redshift')
