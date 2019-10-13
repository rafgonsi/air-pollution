import json
import boto3
import requests
from requests.exceptions import RequestException
from time import sleep
from datetime import datetime
from retrying import retry


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


def download_sensors(stations, wait_between_requests):
    result, failed_stations = [], []
    for station in stations:
        try:
            # print('Downloading sensors for station:', station)
            sensors_info = download_sensors_at_station(station)
            sensors = [dct['id'] for dct in sensors_info]
            result += sensors
        except RequestException as e:
            print(e)  # Just print error message and continue gathering data
            failed_stations.append(station)
        sleep(wait_between_requests)
    if failed_stations:
        print(f'Failed to download sensors for {len(failed_stations)} ' +
              'stations:', failed_stations)
    return result


def download_all_measurements(sensors, wait_between_requests):
    result, failed_sensors = {}, []
    for sensor in sensors:
        try:
            # print('Downloading data for sensor:', sensor)
            measurement = download_measurement(sensor)
            result[sensor] = measurement
        except RequestException as e:
            print(f'Sensor {sensor}:', e)  # Just print error message and continue gathering data
            failed_sensors.append(sensor)
        sleep(wait_between_requests)
    if failed_sensors:
        print(f'Failed to download measurements for {len(failed_sensors)} ' +
              'sensors:', failed_sensors)
    return result


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
