#!/usr/bin/env python
import os
import requests
from datetime import datetime, timezone
from requests.exceptions import HTTPError
from pathlib import Path
from dotenv import load_dotenv

import boto3

# env dev - secrets prd - REMOVED -
dotenv_path = Path('.env/.venv')
load_dotenv(dotenv_path=dotenv_path)
# 
# hidden keys
lastfm_api_key = os.getenv('LASTFM_API_KEY')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')
#aws_region = "us-east-1"
# MinIO env
minio_user = os.getenv('MINIO_ROOT_USER')
minio_pass = os.getenv('MINIO_ROOT_PASSWORD')
minio_endpoint = os.getenv('MINIO_ENDPOINT')
# Postgres env
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_pass = os.getenv('POSTGRES_PASSWORD')
#
warehouse_path = "lastfm-warehouse/"
bucket_name = "lastfm-warehouse"


# Create de database and schematas
def warehouse(env):

    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, IntegerType, StringType, DateType, TimestampType, LongType
    from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform, DayTransform

    if env == "dev":

        from pyiceberg.catalog.sql import SqlCatalog

        catalog = SqlCatalog(
            "lastfm",
            **{
                "uri": f"postgresql+psycopg2://{postgres_user}:{postgres_pass}@localhost/{postgres_db}",
                "warehouse": f"s3://{warehouse_path}",
                "s3.endpoint": minio_endpoint,
                "s3.access-key-id": minio_user,
                "s3.secret-access-key": minio_pass,
            },
        )

    else:

        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
            "default", 
            **{
                "type": "glue",
                "s3.access-key-id": aws_access_key,
                "s3.secret-access-key": aws_secret_key,
                "s3.region": aws_region
                }
            )

    # check if exists
    # catalog.table_exists("docs_example.bids")
    catalog.create_namespace("lastfm") 

     # stage   
    schema = Schema(
        NestedField(field_id=1, name='timestamp', field_type=LongType(), required=False),
        NestedField(field_id=2, name='date_text', field_type=StringType(), required=False),
        NestedField(field_id=3, name='artist', field_type=StringType(), required=False),
        NestedField(field_id=4, name='artist_mbid', field_type=StringType(), required=False),
        NestedField(field_id=5, name='album', field_type=StringType(), required=False),
        NestedField(field_id=6, name='album_mbid', field_type=StringType(), required=False),
        NestedField(field_id=7, name='track', field_type=StringType(), required=False),
        NestedField(field_id=8, name='track_mbid', field_type=StringType(), required=False),
        NestedField(field_id=9, name='date', field_type=DateType(), required=False)
    )


    partition_spec = PartitionSpec(PartitionField(source_id=9, field_id=9, transform=IdentityTransform(), name="date"), spec_id=1)

    catalog.create_table(
        "lastfm.silver_tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 

    # consumption
    schema = Schema(
        NestedField(field_id=1, name='datetime', field_type=TimestampType(), required=False),
        NestedField(field_id=2, name='artist', field_type=StringType(), required=False),
        NestedField(field_id=3, name='album', field_type=StringType(), required=False),
        NestedField(field_id=4, name='track', field_type=StringType(), required=False),
        NestedField(field_id=5, name='date', field_type=DateType(), required=False)
    )


    partition_spec = PartitionSpec(PartitionField(source_id=5, field_id=5, transform=IdentityTransform(), name="date"), spec_id=1)

    catalog.create_table(
        "lastfm.gold_tracks",
        schema=schema,
        partition_spec=partition_spec
    ) 


def drop_table(env):

    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"postgresql+psycopg2://postgres:postgres@localhost/pyiceberg_catalog",
            "warehouse": f"file://{warehouse_path}",
        },
    )  

    catalog.drop_table("silver.tracks")
    catalog.drop_table("gold.tracks")


def query(env, table_name):

    from pyiceberg.catalog.sql import SqlCatalog

    # ToDo only for local...

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"postgresql+psycopg2://postgres:postgres@localhost/pyiceberg_catalog",
            "warehouse": f"file://{warehouse_path}",
        },
    )   

    table = catalog.load_table(('lastfm', table_name))

    #print(table.describe())

    con = table.scan().to_duckdb(table_name=table_name)

    con.sql(
        "SELECT date, COUNT(*) total FROM " + table_name + " GROUP BY date ORDER BY date ASC"
    ).show()
      

#EXTRACT
def extract(env, date):

    try:
        # date to timestamp
        date_from = date + ' 00:00:00'
        date_to = date + ' 23:59:59'

        dt = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S")
        date_from = int(dt.replace(tzinfo=timezone.utc).timestamp())

        dt = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S")
        date_to = int(dt.replace(tzinfo=timezone.utc).timestamp())

        response = requests.get("https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=" + 
                                str(lastfm_api_key) + "&from=" + str(date_from) + "&to=" + str(date_to) + "&format=json")

        response.raise_for_status()
        json_response = response.json()

        pages = int(json_response['recenttracks']['@attr']['totalPages'])
        i = 0
        c = pages

        lists = 'timestamp,date_text,artist,artist_mbid,album,album_mbid,track,track_mbid\n'

        file_name = f"raw/tracks-{date}.csv"

        while i < pages:
            i += 1
            list = get_tracks(start=date_from, end=date_to, page=c)
            c -= 1
            lists = lists + str(list)
            
        if env == 'dev':

            # local file system
            # with open('./lastfm-warehouse/raw/tracks-' + date + '.csv', 'w') as f:
            #     f.write(lists)
            #     f.close()

            s3_client = boto3.client(
                's3',
                endpoint_url=minio_endpoint,
                aws_access_key_id=minio_user,
                aws_secret_access_key=minio_pass,
            )

            try:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=file_name,
                    Body=lists
                )
                message = f"File '{file_name}' uploaded successfully to bucket '{bucket_name}'."
                status = 200
            except Exception as e:
                message = f"Error uploading file: {e}"
                status = 500
        else:
            # put credentials
            s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )

            try:
                s3_client.put_object(
                    Bucket=bucket_name, 
                    Key=file_name, 
                    Body=lists,
                )
                message = f"File '{file_name}' uploaded successfully to bucket '{bucket_name}'."
                status = 200
            except Exception as e:
                message = f"Error uploading file: {e}"
                status = 500

    except HTTPError as http_err:

        message = f'HTTP error occurred: {http_err}'
        status = 500

    except Exception as err:

        message = f'Other error occurred: {err}'
        status = 500
        # error details
        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)

    # return dictionary
    return { 'statusCode' : status, 'body' : message }


def get_tracks(start, end, page):
    
    try:
        response = requests.get('https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=radioheadve&api_key=' + 
                                str(lastfm_api_key) + '&from=' + str(start) + '&to=' +  str(end) + '&page=' + str(page) + '&format=json')
    
        lists = ''

        json_response = response.json()
        # invert list
        tracks = json_response['recenttracks']['track']
        for track in reversed(tracks):

            if "date" in track:

                list = ('"' + track['date']['uts'] + '",' +
                        '"' + track['date']['#text'] + '",' +
                        '"' + track['artist']['#text'] + '",' +
                        '"' + track['artist']['mbid'] + '",' +
                        '"' + track['album']['#text'] + '",' +
                        '"' + track['album']['mbid'] + '",' +
                        '"' + track['name'] + '",' +
                        '"' + track['mbid'] + '"\n')     
                
                lists = lists + list 

        return lists
    
    except HTTPError as http_err:
        message = f'HTTP error occurred ite.: {http_err}'
        status = 500
        return { 'statusCode' : status, 'body' : message }
    except Exception as err:
        message = f'Other error occurred ite.: {err}'
        status = 500
        return { 'statusCode' : status, 'body' : message }

        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #print(exc_type, fname, exc_tb.tb_lineno)


#LOAD
def load(env, date):

    from datetime import date as dt

    
    from pyiceberg.catalog import load_catalog

    from pyiceberg.expressions import NotEqualTo   
    import pyarrow as pa
    import pyarrow.csv as pc
    import pyarrow.fs as fs
    #import pyarrow.compute as pc

    file_name = f"raw/tracks-{date}.csv"
    # Full S3 path to the file
    s3_file_path = f"{bucket_name}/{file_name}"   

    if env == "dev":

        from pyiceberg.catalog.sql import SqlCatalog

        catalog = SqlCatalog(
            "lastfm",
            **{
                "uri": f"postgresql+psycopg2://{postgres_user}:{postgres_pass}@postgres/{postgres_db}",
                "warehouse": f"s3://{warehouse_path}",
                "s3.endpoint": minio_endpoint,
                "s3.access-key-id": minio_user,
                "s3.secret-access-key": minio_pass,
            }
        )
        s3 = fs.S3FileSystem(
                region = aws_region,
                access_key = minio_user,
                secret_key = minio_pass,
                endpoint_override = minio_endpoint
            )

    else:

        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
                "default", 
                **{
                    "type": "glue",
                    #"glue.region": aws_region,
                    #"glue.access-key-id": aws_access_key,
                    #"glue.secret-access-key": aws_secret_key,
                    #"s3.access-key-id": aws_access_key,
                    #"s3.secret-access-key": aws_secret_key,
                    #"s3.region": aws_region,
                    #"profile_name": "default"
                    }
                )

        s3 = fs.S3FileSystem(
                region = aws_region,
                access_key = aws_access_key,
                secret_key = aws_secret_key              
            )  

    # boto3 env aws vars problem
    table = catalog.load_table("lastfm.silver_tracks")


    df = table.scan(
        row_filter = NotEqualTo("date", date)
    ).to_arrow()  

    try:
        # rollback
        table.overwrite(df) 

    except Exception as e:

        message = f"Error deleting data to stage table {e}"
        status = 500
        return { 'statusCode' : status, 'body' : message }

    try:
        with s3.open_input_file(s3_file_path) as file:

            df = pc.read_csv(file)  

    except Exception as e:

        message = f"Error reading CSV file: {e}"
        status = 500
        return { 'statusCode' : status, 'body' : message }


    year, month, day = map(int, date.split('-'))

    df = df.append_column("date", pa.array([dt(year, month, day)] * len(df), pa.date32()))

    try:
        table.append(df)
        rows = len(df)

    except Exception as e:

        message = f"Error appending data to stage table {e}"
        status = 500
        return { 'statusCode' : status, 'body' : message }
    

    message = f"successfully added {rows} rows to stage table."
    status = 200          
    return { 'statusCode' : status, 'body' : message }

#TRANSFORM 
"""
def transformation(env, date):

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.expressions import EqualTo, NotEqualTo

    import pyarrow as pa
    import pyarrow.compute as pc

    catalog = SqlCatalog(
        "lastfm",
        **{
            "uri": f"postgresql+psycopg2://postgres:postgres@localhost/pyiceberg_catalog",
            "warehouse": f"file://{warehouse_path}",
        },
    )   

    silver_table = catalog.load_table(('lastfm', 'silver_tracks'))

    silver_df = silver_table.scan(
        row_filter = EqualTo("date", date),
        selected_fields = ("timestamp","artist","album","track","date"),
    ).to_arrow()

    col = pc.cast(silver_df["timestamp"], pa.timestamp('s'))

    silver_df = silver_df.drop("timestamp")

    silver_df = silver_df.append_column("datetime", col)

    # gold

    gold_table = catalog.load_table(('lastfm', 'gold_tracks'))

    df = gold_table.scan(
        row_filter = NotEqualTo("date", date)
    ).to_arrow()  

    gold_table.overwrite(df) 

    with gold_table.update_schema() as update_schema:
        update_schema.union_by_name(silver_df.schema)    

    gold_table.append(silver_df) 

"""
def transformation(env, date):

    # run dbt model
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    # initialize
    dbt = dbtRunner()

    # add profile.yml

    cli_args = [
        "run",
        "--project-dir", "./dbt/lastfm",
        "--profiles-dir", "./dbt/lastfm",
        "--vars", f"{{'date': '{date}'}}",
        "--target", env
    ]

    try:
        # run the command
        res: dbtRunnerResult = dbt.invoke(cli_args)

        # inspect the results
        for r in res.result:
            print(f"{r.node.name}: {r.status}")

    except Exception as e:

        message = f"Error appending data to final table {e}"
        status = 500
        return { 'statusCode' : status, 'body' : message } 
    
    message = f"successfully added rows to final table."
    status = 200          
    return { 'statusCode' : status, 'body' : message }



# streamlit dashboard

def handler(event, context):
    # step date env
    step = event['step']
    date = event['date']
    env  = event['env']

    match step:
        case "extract":
            response = extract(env, date)
            return {
                'statusCode': response['statusCode'],
                'body': response['body']
            }
        case "load":
            response = load(env, date)
            return {
                'statusCode': response['statusCode'],
                'body': response['body']
            }
        case "transformation":
            response = transformation(env, date)
            return {
                'statusCode': response['statusCode'],
                'body': response['body']
            }


def test():
    import boto3
    import pyarrow.fs as fs
    import pyarrow.parquet as pq
    import pyarrow.csv as pc

    s3 = fs.S3FileSystem(
        region=os.getenv('AWS_REGION'),
        access_key=os.getenv('AWS_ACCESS_KEY'),
        secret_key=os.getenv('AWS_SECRET_KEY')
        #session_token=session_token  # Include the session token if using temporary credentials
    )
    #print("S3 Filesystem:", s3)

    #s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)


    s3_path = "aws-boto3-pipeline-bucket/order_extract.csv"

    # Step 3: Open the file and read it using PyArrow's CSV reader
    with s3.open_input_file(s3_path) as file:
        df = pc.read_csv(file)

    # Display the DataFrame
    print(df)

def test2():

    from datetime import date as dt
    import pyarrow as pa
    import pyarrow.csv as pc
    from pyiceberg.catalog import load_catalog

    # Load the AWS Glue catalog (or AWS Athena catalog)
    catalog = load_catalog('default', 
                          **{
                                "type": "glue",
                                "s3.access-key-id": aws_access_key,
                                "s3.secret-access-key": aws_secret_key,
                                "s3.region": "us-east-1"
                            }
                          )

    # Access a specific Iceberg table in the Glue catalog
    # gold_table = catalog.load_table(('gold', 'tracks'))
    table = catalog.load_table("lastfm.tracks")
    #table = catalog.load_table((schema, 'tracks'))
    #print(table.describe())

    # insert
    df = pc.read_csv("/home/sergio/dev/python/lastfm-api/warehouse/raw/tracks-2024-08-02.csv")

    year, month, day = map(int, "2024-08-02".split('-'))

    df = df.append_column("date", pa.array([dt(year, month, day)] * len(df), pa.date32()))

    table.append(df) 

    con = table.scan().to_arrow()

    print(len(con))
    # Print the schema of the table
    #print(table.schema())

def test3():
    # run dbt model
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    # cli_args = ["run", "--profiles-dir", "./lastfm"]
    cli_args = ["run", "--project-dir", "./lastfm"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")

"""
if __name__ == '__main__':
    #warehouse()
    date = '2024-08-05'
    env = 'prd'

    #print(extract(env, date))
    #print(load(env, date))
    print(transformation(env, date))

    #query(env, 'gold_tracks')
    #query('gold')
    #test3()
"""




