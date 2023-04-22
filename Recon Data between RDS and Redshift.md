# Recon Data between RDS and Redshift

Redshift does not enforce uniqueness in its primary keys. Due to bug in DMS, the data migrated into Redshift may have missing or duplicate data compared to source RDS tables. 

This solution provides a simple way to reconciliate the data between a table in RDS and a table in Redshift.

* Export ID from a RDS table into a S3 bucket.
* Export ID from a Redshift table into a S3 bucket.
* Compare the exported data between RDS and Redshift.



## Steps

1. Create/select an S3 bucket for exported data.

```python
REGION="ap-southeast-1"
S3_BUCKET = 'temp-460453255610'
```

2. Set the configuration for RDS table. For security, consider to store username and password in AWS Secret Manager.

```python
# Export data from RDS to S3 Bucket
RDS_ENDPOINT="database-3.cluster-c3bottoj4h9o.ap-southeast-1.rds.amazonaws.com"
RDS_DB="stackoverflow"
RDS_USER="admin"
RDS_PASSWD = "Qwer!234"
RDS_PREFIX = f'rds_{datetime_str}'
RDS_QUERY = 'select id from posts'
s3_path_rds = f's3-{REGION}://{S3_BUCKET}/{RDS_PREFIX}'
```

3. Set the configuration for Redshift table.

```python
# Export data from Redshift to S3 Bucket
REDSHIFT_HOST = 'redshift-cluster-1.cs21sivqdtax.ap-southeast-1.redshift.amazonaws.com'
REDSHIFT_DB = 'dev'
REDSHIFT_USER = 'awsuser'
REDSHIFT_PASSWD = 'Qwer!234'
REDSHIFT_PREFIX = f'redshift_{datetime_str}'
REDSHIFT_QUERY = 'select id from stackoverflow.posts'
REDSHIFT_IAM_ROLE = 'arn:aws:iam::460453255610:role/RedshiftImportFromS3'
s3_path_redshift = f's3://{S3_BUCKET}/{REDSHIFT_PREFIX}'
```

4. Export the RDS table using SELECT INTO S3. 

```python
print('Select from RDS into S3...')
select_rds_into_s3(RDS_ENDPOINT, RDS_USER, RDS_PASSWD, RDS_DB, RDS_QUERY, s3_path_rds)
```

5. Export the Redshift table using UNLOAD.

```python
print('Unload from Redshift into S3...')
unload_redshift_to_s3(REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWD, REDSHIFT_QUERY, s3_path_redshift, REDSHIFT_IAM_ROLE, REGION)
```

6. Exported data are saved in S3 bucket.
   * You can consider to add folder in the `RDS_PREFIX` and `REDSHIFT_PREFIX` so that files are put into folders.

![image-20230406190309972](./assets/Recon%20Data%20between%20RDS%20and%20Redshift.assets/image-20230406190309972.png)

7. Download files from S3 and read them into 2 separate list.

```python
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
```

8. Convert them into sets and find the difference between them.

```python
print(len(rds_data), len(redshift_data))

only_in_rds = set(rds_data).difference(set(redshift_data))
print('Only in RDS:', len(only_in_rds))

only_in_redshift = set(redshift_data).difference(set(rds_data))
print('Only in Redshift:', len(only_in_redshift))
```

9. Examine the data in

![image-20230406190814842](./assets/Recon%20Data%20between%20RDS%20and%20Redshift.assets/image-20230406190814842.png)

10. if there is difference in data, an exception will be thrown, which is used ful for lambda deployment.

![image-20230406191044976](./assets/Recon%20Data%20between%20RDS%20and%20Redshift.assets/image-20230406191044976.png)



## Considerations

1. If the query contains a where statement, consider to set the columns in where statement as a key, which will speed up the performance.
2. The **SELECT INTO S3** may have an impact on RDS. You may consider to add a read-replica for RDS if you are concern about the performance impact.

