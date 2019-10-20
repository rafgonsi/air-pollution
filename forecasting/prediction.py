from preprocessing import make_dataset


if __name__ == '__main__':
    BUCKET = 'air-pollution-data-pl'
    ARCHIVE_FOLDER = 'historical-raw-data'
    PARQUET_DATA_FILENAME = 'data.parquet'
    PREFIX_FRESH_DATA = 'data'

    df = make_dataset(Bucket=BUCKET,
                      parquet_data_filename=PARQUET_DATA_FILENAME,
                      prefix_fresh_data=PREFIX_FRESH_DATA,
                      archive_folder=ARCHIVE_FOLDER)
