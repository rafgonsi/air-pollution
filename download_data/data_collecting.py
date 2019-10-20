import json
import boto3
import requests
from requests.exceptions import RequestException
from time import sleep
from datetime import datetime
from retrying import retry
from inspect import signature


def exec_request(url, timeout):
    """Executes request given in url and returns a dictionary with content"""
    data = requests.get(url, timeout=timeout)
    data.raise_for_status()  # Raise in case of failed status
    return data.json()


@retry(stop_max_attempt_number=2, wait_fixed=60)
def download_stations_info():
    """Returns a dictionary with all available information about all stations"""
    url = 'http://api.gios.gov.pl/pjp-api/rest/station/findAll'
    return exec_request(url, timeout=20)


@retry(stop_max_attempt_number=2, wait_fixed=15)
def download_measurement(sensor_id):
    url = f'http://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}'
    return exec_request(url, timeout=15)


@retry(stop_max_attempt_number=2, wait_fixed=20)
def download_sensors_at_station(station_id):
    """Downloads information about available sensors at station `station_id`."""
    url = f'http://api.gios.gov.pl/pjp-api/rest/station/sensors/{station_id}'
    return exec_request(url, timeout=20)


def get_available_stations(station_info):
    return set(station['id'] for station in station_info)


def get_func_signature(f):
    return f.__name__ + str(signature(f))


def download(ids, wait_between_requests, download_data_func,
             update_result_func):
    failed_ids = []
    result = None
    for id in ids:
        try:
            data = download_data_func(id)
            result = update_result_func(result, data, id)
        except RequestException:
            failed_ids.append(id)
        sleep(wait_between_requests)
    if failed_ids:
        print(f'Failed to run {get_func_signature(download_data_func)} '
              f'{len(failed_ids)} times. '
              f'Argument values for failed cases: {failed_ids}.')
    return result


def download_sensors(stations, wait_between_requests):
    def update_result(result, downloaded_data, _):
        # third arg is for compatibility only
        sensors_at_station = [dct['id'] for dct in downloaded_data]
        return result + sensors_at_station if result else sensors_at_station

    return download(
        ids=stations,
        wait_between_requests=wait_between_requests,
        download_data_func=download_sensors_at_station,
        update_result_func=update_result
    )


def download_measurements(sensors, wait_between_requests):
    def update_result(result, downloaded_data, sensor):
        return {**result, sensor: downloaded_data} if result \
            else {sensor: downloaded_data}

    return download(
        ids=sensors,
        wait_between_requests=wait_between_requests,
        download_data_func=download_measurement,
        update_result_func=update_result
    )


def get_now_str():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def dct_to_s3(dct, Bucket, Key, ACL='private'):
    client = boto3.client('s3')
    client.put_object(
        Body=json.dumps(dct),
        ACL=ACL,
        Bucket=Bucket,
        Key=Key
    )


if __name__ == '__main__':
    BUCKET = 'air-pollution-data-pl'
    WAIT_BETWEEN_REQUESTS = 3

    print('Starting data download:', datetime.now())
    now = get_now_str()
    stations_info = download_stations_info()
    stations = get_available_stations(stations_info)
    sensors = download_sensors(stations, WAIT_BETWEEN_REQUESTS)
    measurements = download_all_measurements(sensors, WAIT_BETWEEN_REQUESTS)
    dct_to_s3(measurements,
              Bucket=BUCKET,
              Key=f'data_{now}.json')
    print('Done:', datetime.now())
