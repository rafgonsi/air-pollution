from inspect import signature


def get_func_signature(f):
    return f.__name__ + str(signature(f))


def download(ids, wait_between_requests, initial_result, download_data_func, update_result_func):
    failed_ids = []
    result = initial_result  # copy? - nie, to i tak bedzie nadpisywane
    for id in ids:
        try:
            print('#TODO')
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
    def update_result(result, downloaded_data, station):
        sensort_at_station = [dct['id'] for dct in sensors_info]
        return result + sensort_at_station

    return download(
        ids=stations,
        wait_between_requests=wait_between_requests,
        initial_result=[],
        download_data_func=download_sensors_at_station,
        update_result_func=update_result
    )


def download_measurements(sensors, wait_between_requests):
    def update_result(result, downloaded_data, sensor):
        return {**result, sensor: downloaded_data}

    return download(
        ids=sensors,
        wait_between_requests=wait_between_requests,
        initial_result={},
        download_data_func=download_measurement,
        update_result_func=update_result
    )
