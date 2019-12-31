"""
Athena script to Drop Table
"""
import os
import sys
import time
import re
import random
import logging
from logging.handlers import TimedRotatingFileHandler
import json
import urllib.request
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ATHENA_LOG = os.environ['ATHENA_LOG']
ATHENA_LOG_PREFIX = os.environ['ATHENA_LOG_PREFIX']
DATABASE_NAME = os.environ['DATABASE_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
ATHENA_OPERATION = os.environ['ATHENA_OPERATION']
try:
    OUTPUT_BUCKET_NAME = os.environ['OUTPUT_BUCKET_NAME']
except KeyError as err:
    OUTPUT_BUCKET_NAME = None

try:
    TARGET_PATH_NAME = os.environ['TARGET_PATH_NAME']
except KeyError as err:
    TARGET_PATH_NAME = None

LOG_FILE = "/APP/athena-partition.log"

"""
Setup Logging
"""
LOGFORMAT = '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s'
FORM = logging.Formatter(LOGFORMAT)
logging.basicConfig(
    format=LOGFORMAT,
    level=logging.INFO
)
LOGGER = logging.getLogger()
if LOGGER.hasHandlers():
    LOGGER.handlers.clear()
LOGHANDLER = TimedRotatingFileHandler(LOG_FILE, when="midnight", interval=1, backupCount=7)
LOGHANDLER.suffix = "%Y-%m-%d"
LOGHANDLER.setFormatter(FORM)
LOGGER.addHandler(LOGHANDLER)
CONSOLEHANDLER = logging.StreamHandler()
CONSOLEHANDLER.setFormatter(FORM)
LOGGER.addHandler(CONSOLEHANDLER)
LOGGER.info("Starting")
CONFIG = Config(
    retries=dict(
        max_attempts=10
    )
)

S3 = boto3.client('s3')
ATHENA = boto3.client('athena', config=CONFIG)
GLUE = boto3.client('glue', config=CONFIG)

def error_handler(lineno, error):
    """
    Error Handler

    Can submit Cloudwatch events if LOG_GROUP_NAME and LOG_STREAM_NAME are set.
    """

    LOGGER.error('The following error has occurred on line: %s', lineno)
    LOGGER.error(str(error))

def send_message_to_slack(text):
    """
    Formats the text provides and posts to a specific Slack web app's URL
    Args:
        text : the message to be displayed on the Slack channel
    Returns:
        Slack API repsonse
    """

    try:
        post = {
            "text": ":fire: :sad_parrot: An error has occured in the *FMS Table \
             Maintenace* pod :sad_parrot: :fire:",
            "attachments": [
                {
                    "text": "{0}".format(text),
                    "color": "#B22222",
                    "attachment_type": "default",
                    "fields": [
                        {
                            "title": "Priority",
                            "value": "High",
                            "short": "false"
                        }
                    ],
                    "footer": "Kubernetes API",
                    "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png"
                }
            ]
            }

        ssm_param_name = 'slack_notification_webhook'
        ssm = boto3.client('ssm', config=CONFIG)
        try:
            response = ssm.get_parameter(Name=ssm_param_name, WithDecryption=True)
        except ClientError as err:
            if err.response['Error']['Code'] == 'ParameterNotFound':
                LOGGER.info('Slack SSM parameter %s not found. No notification sent', \
                ssm_param_name)
            else:
                LOGGER.error("Unexpected error when attempting to get Slack webhook URL: %s", err)
            return
        if 'Value' in response['Parameter']:
            url = response['Parameter']['Value']
            json_data = json.dumps(post)
            req = urllib.request.Request(
                url,
                data=json_data.encode('ascii'),
                headers={'Content-Type': 'application/json'})
            LOGGER.info('Sending notification to Slack')
            response = urllib.request.urlopen(req)

        else:
            LOGGER.info('Value for Slack SSM parameter %s not found. No notification sent', \
            ssm_param_name)
            return

    except Exception as err:
        LOGGER.error(
            'The following error has occurred on line: %s',
            sys.exc_info()[2].tb_lineno)
        LOGGER.error(str(err))

def clear_down(sql):
    """
    After an Athena failure, delete the target path before the sql is retried

    Args:
        sql         : the SQL to execute
    Returns:
        None
    """

    try:
        full_path = sql.split('s3://')[1].split("'")[0]
        bucket_name = full_path.split('/')[0]
        path_to_delete = '/'.join(full_path.split('/')[1:])

        LOGGER.info(
            'Attempting to delete %s from bucket %s',
            path_to_delete,
            bucket_name)

        bucket = S3.Bucket(bucket_name)
        response = bucket.objects.filter(Prefix=path_to_delete).delete()
        LOGGER.info(response)

        if not response:
            LOGGER.info('Nothing to delete')
        else:
            LOGGER.info('The following was deleted: %s', response[0]['Deleted'])

    except Exception as err:
        send_message_to_slack(err)
        error_handler(sys.exc_info()[2].tb_lineno, err)

def check_query_status(execution_id):
    """
    Loop until the query is either successful or fails

    Args:
        execution_id             : the submitted query execution id

    Returns:
        None
    """
    try:
        client = boto3.client('athena', config=CONFIG)
        LOGGER.debug('About to check Athena status on SQL')
        while True:
            response = client.get_query_execution(
                QueryExecutionId=execution_id)
            if response['QueryExecution']['Status']['State'] in ('FAILED', 'SUCCEEDED', 'CANCELLED'):
                return response
            LOGGER.debug('Sleeping for 1 second')
            time.sleep(1)

    except Exception as err:
        send_message_to_slack(err)
        error_handler(sys.exc_info()[2].tb_lineno, err)

def execute_athena(sql, database_name):
    """
    Run SQL on Athena.

    Args:
        sql             : the SQL to execute
        conditions      : dict of optional pre and post execution conditions
        output_location : the S3 location for Athena to put the results

    Returns:
        response        : response of submitted Athena query
    """
    try:
        attempts = 8
        i = 1
        while True:
            if i == attempts:
                LOGGER.error('%s attempts made. Failing with error', attempts)
                sys.exit(1)
            try:
                response = ATHENA.start_query_execution(
                    QueryString=sql,
                    QueryExecutionContext={
                        'Database': database_name
                        },
                    ResultConfiguration={
                        'OutputLocation': "s3://" + ATHENA_LOG,
                        }
                    )
            except ClientError as err:
                if err.response['Error']['Code'] in (
                        'TooManyRequestsException',
                        'ThrottlingException',
                        'SlowDown'):
                    LOGGER.info('athena.start_query_execution throttled. Waiting %s second(s) \
                     before trying again', 2 ** i)
                    time.sleep((2 ** i) + random.random())
                else:
                    raise err
                i += 1
            else:
                LOGGER.debug('Athena query submitted. Continuing.')
                LOGGER.debug(response)
                response = check_query_status(response['QueryExecutionId'])
                if response['QueryExecution']['Status']['State'] == 'CANCELLED':
                    LOGGER.warning(response)
                    LOGGER.info('SQL query cancelled. Waiting %s second(s) \
                    before trying again', 2 ** i)
                    time.sleep((2 ** i) + random.random())
                    i += 1
                    clear_down(sql)
                elif response['QueryExecution']['Status']['State'] == 'FAILED':
                    LOGGER.warning(response)
                    state_change_reason = response['QueryExecution']['Status']['StateChangeReason']
                    compiled = re.compile("Table*does not exist")
                    compiled_not_found = re.compile("Table not found*")
                    if "Query exhausted resources at this scale factor" in state_change_reason \
                       or "Partition metadata not available" in state_change_reason \
                       or "INTERNAL_ERROR" in state_change_reason \
                       or "ABANDONED_QUERY" in state_change_reason \
                       or "HIVE_PATH_ALREADY_EXISTS" in state_change_reason \
                       or "HIVE_CANNOT_OPEN_SPLIT" in state_change_reason \
                       or compiled.match(state_change_reason) \
                       or compiled_not_found.match(state_change_reason):
                        LOGGER.debug('SQL query failed. Waiting %s second(s) before trying again', \
                        2 ** i)
                        time.sleep((2 ** i) + random.random())
                        i += 1
                        clear_down(sql)
                    if "Table not found" in state_change_reason:
                        LOGGER.warning('Database / Table not found, continuing.')
                        LOGGER.warning(sql)
                        send_message_to_slack('Database / Table not found')
                        sys.exit(1)
                    else:
                        send_message_to_slack('SQL query failed and this type of error will not \
                        be retried. Exiting with failure.')
                        LOGGER.error('SQL query failed and this type of error will not be retried. \
                        Exiting with failure.')
                        sys.exit(1)
                elif response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                    LOGGER.info('SQL statement completed successfully')
                    break

    except Exception as err:
        send_message_to_slack(err)
        error_handler(sys.exc_info()[2].tb_lineno, err)

    return response

def check_table(database_name, table_name):
    """
    Checks for the existence of a table in the Glue catalogue.

    Args:
        database_name  : the schema name in Athena
        table_name     : the table name in Athena
    Returns:

        Glue response in JSON format
    """

    try:
        response = GLUE.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        return response
    except ClientError as err:
        if err.response['Error']['Code'] in 'EntityNotFoundException':
            err = 'Table ' + database_name + '.' + table_name + ' not found!'
            LOGGER.warning(err)
            return False
        else:
            send_message_to_slack(err)
            error_handler(sys.exc_info()[2].tb_lineno, err)

def substitute_params(sql, sql_params):
    """
    Substitute SQL dict of parameter values

    Args:
        sql              : sql statement
        sql_params       : dict of parameters to substitute

    Returns:
        string containing SQL with parameters substituted in
    """

    for param_name in sql_params:
        sql = sql.replace(('{' + '{0}'.format(param_name) + '}'),
                          sql_params[param_name])
    return sql

def read_sql_file(sql_filename, sql_params):
    """
    Read SQL from file and substitute parameters.

    Args:
        sql_filename     : the filename
        sql_params       : dict of parameters to substitute

    Returns:
        tuple string containing SQL with parameters substituted in
        followed by dict of conditions for SQL execution
    """

    try:
        file = open(sql_filename, "r")
        LOGGER.info('Opened the sql file %s', sql_filename)
        sql = file.read()
        #(sql, conditions) = extract_conditions(sql)
        sql = substitute_params(sql, sql_params)
        LOGGER.info('Replaced parameters.')
        #return (sql, conditions)
        return sql
    except Exception as err:
        error_handler(sys.exc_info()[2].tb_lineno, err)

def main():
    """
    Main function to execute Athena queries
    """

    if ATHENA_OPERATION == 'DROP':
        drop_table_sql = "DROP TABLE IF EXISTS " + DATABASE_NAME + "." + TABLE_NAME + ";"
        try:
            LOGGER.info('Dropping Table from "%s.%s"', DATABASE_NAME, TABLE_NAME)
            LOGGER.info(drop_table_sql)
            execute_athena(drop_table_sql, DATABASE_NAME)
        except Exception as err:
            send_message_to_slack(err)
            error_handler(sys.exc_info()[2].tb_lineno, err)
            sys.exit(1)
    elif ATHENA_OPERATION == 'CREATE':
        print("Checking if Table Exists Already")
        create_table = check_table(DATABASE_NAME, TABLE_NAME)
        if create_table:
            #table already exists. Don't proceed with Create
            LOGGER.info("Operation Cannot be Performed. %s.%s already exists.", \
            DATABASE_NAME, TABLE_NAME)
        else:
            sql_params = {
                'bucket-name': OUTPUT_BUCKET_NAME,
                'table-name': TABLE_NAME.lower(),
                'database-name': DATABASE_NAME,
                'target-path-name': TARGET_PATH_NAME}
            dirname = os.path.dirname(__file__)
            sql_file = os.path.join(dirname, 'sql/create_table.sql')
            create_table_sql = read_sql_file(sql_file, sql_params)
            msck_table_sql = "MSCK REPAIR TABLE " + DATABASE_NAME + "." + TABLE_NAME + ";"
            try:
                LOGGER.info('Creating Table "%s.%s"', DATABASE_NAME, TABLE_NAME)
                LOGGER.info(create_table_sql)
                execute_athena(create_table_sql, DATABASE_NAME)
                # Execute msck repair table.
                execute_athena(msck_table_sql, DATABASE_NAME)
            except Exception as err:
                send_message_to_slack(err)
                error_handler(sys.exc_info()[2].tb_lineno, err)
                sys.exit(1)
    else:
        LOGGER.info("Operation Cannot be Performed.Option is neither DROP NOR CREATE.")
        # Operation cannot be performed as it is neither CREATE nor DROP Table.

    LOGGER.info("We are done here.")

if __name__ == '__main__':
    main()
