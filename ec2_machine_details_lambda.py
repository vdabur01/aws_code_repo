import os
import csv
import logging
import boto3
import io
import pandas as pd
import psycopg2
from datetime import datetime, date
from geomart.geomart_secret_manager import AwsSecretManger


current_date = date.today()

# Global environment variable
if os.environ.get("ENV"):
    env = os.environ.get("ENV").lower()
else:
    env = "LOCAL"

# Constants
AWS_REGION = 'us-west-2'
S3_BUCKET_PREFIX = 'geomartcloud-data-'
AWS_ACCT_PATH = '/tmp/ec2.csv'
REGIONS = [AWS_REGION]
SECRET_NAME = 'devagsmonitor/db-qamonitoradmin'
TABLE_NAME = 'dev_ec2_list'

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class EC2Table(object):
    def __init__(self):
        self.rows = []
        self.instance_id_map = {}
        self.private_ipv4_map = {}
        self.public_ipv4_map = {}

    def add_row(self, row):
        self.rows.append(row)
        self.instance_id_map[row['InstanceId']] = row
        self.private_ipv4_map[row['Private_ipv4']] = row
        if 'Public_ipv4' in row and row['Public_ipv4']:
            self.public_ipv4_map[row['Public_ipv4']] = row

    def get_rows(self):
        return self.rows

    def get_row_instance_id(self, instance_id):
        return self.instance_id_map.get(instance_id)

    def get_row_private_ipv4(self, ip_address):
        return self.private_ipv4_map.get(ip_address)

    def get_row_public_ipv4(self, ip_address):
        return self.public_ipv4_map.get(ip_address)

    def contains_instance_id(self, instance_id):
        return instance_id in self.instance_id_map

    def contains_private_ipv4(self, ip_address):
        return ip_address in self.private_ipv4_map

    def contains_public_ipv4(self, ip_address):
        return ip_address in self.public_ipv4_map

def configure_logging():
    logging.basicConfig(level=logging.INFO)

def dict_format(tag_list):
    tmp_dict = {}
    if tag_list:
        for tag in tag_list:
            tmp_dict[tag['Key']] = tag['Value']
    return tmp_dict

def db_handler(secret_name, aws_region_name, aws_profile_name, env):
    secret_response = AwsSecretManger(aws_region_name, secret_name, aws_profile_name, env)
    secret_data = secret_response.getSecretKey()
    db_user = secret_data['username']
    db_password = secret_data['password']
    db_host = secret_data['primarydb']
    database_name = secret_data['dbname']
    conn = psycopg2.connect(
        dbname=database_name,
        user=db_user,
        password=db_password,
        host=db_host
    )
    return conn

def inventory_details():
    try:
        result = []
        logging.info('EC2 Inventory details')

        for region in REGIONS:
            ec2 = boto3.client('ec2', region_name=region)
            response = ec2.describe_instances()
            for item in response["Reservations"]:
                for instance in item['Instances']:
                    result.append(instance.get('InstanceId', ''))
        return result
    except Exception as e:
        logging.error(f'EC2 inventory with uncaught exception: {e}')

def create_all_instance_dict(all_instance_table):
    ec2_resource = boto3.resource('ec2', region_name=AWS_REGION)
    running_instances = ec2_resource.instances.all()
    for instance in running_instances:
        info_dict = {
            'InstanceId': instance.id,
            'Public_ipv4': instance.public_ip_address,
            'Private_ipv4': instance.private_ip_address,
            'InstanceType': instance.instance_type,
            'InstanceState': instance.state["Name"],
            'SecurityGroup': instance.security_groups,
            'SubnetId': instance.subnet_id,
        }
        info_dict.update(dict_format(instance.tags))
        all_instance_table.add_row(info_dict)

def prepare_required_cols(search_instance_list, all_instance_table, search_table):
    for instance_id in search_instance_list:
        if all_instance_table.contains_instance_id(instance_id):
            search_table.add_row(all_instance_table.get_row_instance_id(instance_id))
    columns = list(set().union(*[row.keys() for row in search_table.get_rows()]))
    columns.sort()
    headers = ['InstanceId', 'Name', 'InstanceType', 'InstanceState', 'SecurityGroup', 'SubnetId']
    columns = [col for col in columns if col not in headers]
    columns = ['InstanceId', 'Name', 'InstanceType', 'InstanceState', 'SecurityGroup', 'SubnetId'] + columns
    return columns

def write_to_csv(columns, search_table):
    logging.info(f'Writing result to {AWS_ACCT_PATH}')
    with open(AWS_ACCT_PATH, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns)
        for row in search_table.get_rows():
            writer.writerow([row.get(column, None) for column in columns])

def read_data_from_s3(bucket_name, key_path):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    response = s3.get_object(Bucket=bucket_name, Key=key_path)
    data = response['Body'].read()
    df = pd.read_csv(io.BytesIO(data))
    return df

def correct_csv_data(df):
    pattern = r'Environment'
    def merge_env_coumns(row):
        merged_value = ""
        selected_columns = row.filter(regex=pattern)
        selected_values = selected_columns.dropna()
        if len(selected_values) > 0:
            merged_value = selected_values.iloc[0]
        return merged_value

    # Correct AppID
    df['AppID'] = df[['AppID', 'Appid', 'AppId']].fillna('').sum(axis=1)
    df.drop(columns=['AppId', 'Appid'], inplace=True)

    # Correct Created By
    df['CreatedBy'] = df[['CreatedBy', 'Createdby']].fillna('').sum(axis=1)
    df.drop(columns=['Createdby'], inplace=True)

    # Correect Order Column
    df['Order_values'] = df['Order'].fillna(df['order']).astype('Int64')
    df.drop(columns=['Order', 'order'], inplace=True)

    #correct Environment Column
    df['Env_value'] = df.filter(regex=pattern).apply(merge_env_coumns, axis=1)
    df.drop(columns=df.filter(regex=pattern).columns, inplace=True)

    #rename column with hypen to underscore
    df.columns = df.columns.str.replace("-", "_")
    df.columns = df.columns.str.replace(":", "_")
    return df

def save_data_to_postgres(df):
    conn = db_handler(SECRET_NAME, AWS_REGION, "default", env)
    try:
        cursor = conn.cursor()
        columns = ", ".join([f"{col} TEXT" for col in df.columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns});"
        cursor.execute(create_table_sql)

        get_data_query = f"SELECT * FROM {TABLE_NAME}"
        df_db = pd.read_sql(get_data_query, conn)

        existing_ids = set(df_db['InstanceId']) if 'InstanceId' in df_db.columns else set()
        new_data = df[~df['InstanceId'].isin(existing_ids)]

        with open(AWS_ACCT_PATH, 'r') as csv_file:
            if len(existing_ids):
                records_to_insert = new_data.to_dict('records')
            else:
                records_to_insert = csv.DictReader(csv_file)

            for row in records_to_insert:
                row['Timestamp'] = datetime.now()
                columns = ', '.join(row.keys())
                values = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values})"
                cursor.execute(insert_query, list(row.values()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def lambda_handler(event, context):
    configure_logging()
    logging.info('Retrieving EC2 info...')
    search_instance_list = inventory_details()

    logging.info('Processing...')
    all_instance_table = EC2Table()
    search_table = EC2Table()

    create_all_instance_dict(all_instance_table)
    columns = prepare_required_cols(
        search_instance_list, all_instance_table, search_table)

    write_to_csv(columns, search_table)

    s3_bucket_name = f'{S3_BUCKET_PREFIX}{env}'
    s3_key_path = f'ec2_list/ec2_{str(current_date)}.csv'
    s3 = boto3.client('s3', region_name=AWS_REGION)
    s3.upload_file(AWS_ACCT_PATH, s3_bucket_name, s3_key_path)

    df = read_data_from_s3(s3_bucket_name, s3_key_path)
    df = correct_csv_data(df)
    df['Timestamp'] = datetime.now()

    save_data_to_postgres(df)

    return {
        'statusCode': 200,
        'body': 'Data imported successfully'
    }

if __name__ == '__main__':
    lambda_handler(None, None)
