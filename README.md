# FMS-Table-Maintenance
The repo contains code that can aid in executing Athena queries to CREATE or DROP the table fms_prod.voyage_status.

[![Docker Repository on Quay](https://quay.io/repository/ukhomeofficedigital/FMS-Table-Maintenance "Docker Repository on Quay")](https://quay.io/repository/ukhomeofficedigital/FMS-Table-Maintenance)

## Introduction
This app

## Dependencies

- Docker
- Kubernetes
- Python3.7
- Drone
- AWS CLI
- Slack
- AWS Keys with access to Athena, Glue and S3

## Variables
See below a list of variables that are required, and also some that are optional

|  Variable name           |    example    | description                                                                                     | required |
| ------------------------ | ------------- | ------------------------------------------------------------------------------------------------| -------- |
|    ATHENA_LOG            | s3-athena-log | Location of Athena log files                                                                    |    Y     |
|    CSV_S3_BUCKET         | s3-bucket-csv | S3 bucket that contains the CSV file                                                            |    Y     |
|    CSV_S3_FILE           | file.csv      | Location of the CSV file. Can be a filename, or a prefix + filename (a/path/to/csv.file)        |    Y     |
|    AWS_ACCESS_KEY_ID     | ABCD          | AWS access key ID                                                                               |    Y     |
|    AWS_SECRET_ACCESS_KEY | ABCD1234      | AWS secret access key                                                                           |    Y     |
|    AWS_DEFAULT_REGION    | eu-west-2     | AWS default region                                                                              |    Y     |    

## Example usage
### Running in Docker

Build container
```
docker build -t fmstable app/

```

Run container to Create Table
```
docker run -e ATHENA_LOG=s3-athena-log -e ATHENA_LOG_PREFIX=partdrop -e AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxx -e AWS_SECRET_ACCESS_KEY=xxxxxxxxx -e AWS_DEFAULT_REGION=eu-west-2 -e DATABASE_NAME="fms_notprod" -e TABLE_NAME="voyage_status" -e ATHENA_OPERATION="CREATE" -e OUTPUT_BUCKET_NAME="s3://s3-dq-fms-working-notprod" -e TARGET_PATH_NAME="2019-03-05" fmstable

```

Run container to Drop Table
```
docker run -e ATHENA_LOG=s3-athena-log -e ATHENA_LOG_PREFIX=partdrop -e AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxxx -e AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxx -e AWS_DEFAULT_REGION=eu-west-2 -e DATABASE_NAME="fms_notprod" -e TABLE_NAME="voyage_status" -e ATHENA_OPERATION="DROP" -e OUTPUT_BUCKET_NAME="s3-dq-fms-working-notprod" -e TARGET_PATH_NAME="2019-03-05/15-01-01-12345" fmstable


```
