# air-pollution
Gathering air pollution data and forecasting

The purpose of this project is to make air pollution forecasts using data from sensors located in different parts of Poland. Solution is built using Amazon Web Services.


## Downloading data
We use data from Chief Inspectorate Of Environmental Protection of Poland (http://www.gios.gov.pl/). They deliver hourly measurements from sensors spread across the country and able to measure 7 types of pollution (as for today: 21.10.2019): PM10, PM2.5, SO<sub>2</sub>, NO<sub>2</sub>, CO, C<sub>6</sub>H<sub>6</sub>, O<sub>3</sub>. The data is served in json format and can be accessed via http request.

Architecture of system responsible for downloading that data is presented below.

<p align="center">
  <img src="https://github.com/rafgonsi/air-pollution/blob/master/figures/downloading_architecture.png" />
</p>

Once a day Cloudwatch event triggers Lambda that starts EC2 instance. The instance has its startup script that starts downloading measurement data from GIOŚ. After it finishes, the data is saved to S3 Bucket.

## Forecasting (in progress)
Forecasting time series with Facebook Prophet model.

## Future work
- set up DASK cluster on AWS and run forecasts on it
- experiment with distinct time series models
- gather actual weather data and weather forecasts and use them to build features for time series modeling
- prepare forecasts visualization
