from preprocessing import make_dataset


if __name__ == '__main__':
    df = make_dataset(Bucket='air-pollution-data-pl',
                      parquet_data_filename='data.parquet',
                      prefix_fresh_data='data',
                      archive_folder='historical-raw-data')
