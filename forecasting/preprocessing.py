import pandas as pd
import json
from itertools import chain
import boto3


def flatten_measurements_dict(dct):
    """Returns generator of tuples
        (sensor_id, type of pollution, datetime, value of pollution).
        Does not include stations that have no data."""
    return (
        (sensor_id, measurements['key'],
         pd.Timestamp(measurement['date']), measurement['value'])
        for sensor_id, measurements in dct.items()
        for measurement in measurements['values']
    )


def list_jsons_from_s3(Bucket, Prefix):
    """Lists all json files in given S3 Bucket which name starts with Prefix"""
    client = boto3.client('s3')
    s3_content = client.list_objects(Bucket=Bucket, Prefix=Prefix)
    return [fileinfo['Key'] for fileinfo in s3_content['Contents']
            if '.json' in fileinfo['Key']]


def load_single_json(filename, Bucket):
    client = boto3.client('s3')
    content = client.get_object(Bucket=Bucket, Key=filename)['Body'].read()
    return json.loads(content)


def move_files_on_s3(files, Bucket, destination):
    client = boto3.client('s3')
    for file in files:
        client.copy_object(Bucket=Bucket,
                           CopySource=f'{Bucket}/{file}',
                           Key=f'{destination}/{file}')
        client.delete_object(Bucket=Bucket, Key=file)


def get_data_dicts(Bucket, Prefix='', archive_folder=None):
    """Downloads json files from S3 `Bucket`. Download only files with specified
    `Prefix` (all files by default). If `archive_folder` is specified, move
    those files to that folder."""
    json_files = list_jsons_from_s3(Bucket, Prefix=Prefix)
    data_dicts = [load_single_json(file, Bucket) for file in json_files]
    if archive_folder:
        move_files_on_s3(files=json_files,
                         Bucket=Bucket,
                         destination=archive_folder)
    return data_dicts


def data_dicts_to_df(data_dicts):
    flattened_dicts = (flatten_measurements_dict(dct) for dct in data_dicts)
    flattened_data = chain.from_iterable(flattened_dicts)
    return pd.DataFrame(flattened_data,
                        columns=['sensor_id', 'pollution_type',
                                 'date', 'pollution_value'])


def make_dataset(Bucket, parquet_data_filename, prefix_fresh_data='',
                 archive_folder=None):
    """
    Loads data from S3 `Bucket`:
    - jsons with prefix `prefix_fresh_data',
    - parquet file `parquet_data_filename` that contains already merged jsons
    (ignored if file does not exist).
    Then merges jsons into a data frame and appends to it parquet data. Replaces
    parquet file on S3 by newly created data frame converted to parquet format.
    If archive folder is specified, moves loaded jsons to that folder in the
    bucket.

    Returns data frame with the following columns: sensor_id, pollution_type,
    date, pollution_value
    """
    data_dicts = get_data_dicts(
        Bucket,
        archive_folder=archive_folder,
        Prefix=prefix_fresh_data
    )
    df_new = data_dicts_to_df(data_dicts).dropna()
    try:
        df_old = pd.read_parquet(f's3://{Bucket}/{parquet_data_filename}')
    except PermissionError:  # File does not exist
        df_old = pd.DataFrame()
    result = (pd.concat([df_new, df_old], ignore_index=True)
              .sort_values('date')
              .drop_duplicates(subset=['sensor_id', 'pollution_type', 'date'],
                               keep='last'))  # keep the freshest data
    result.to_parquet(f's3://{Bucket}/{parquet_data_filename}', index=False)
    return result



