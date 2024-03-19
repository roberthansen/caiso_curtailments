# Introduction
This project aims to model derates for California's thermal power plants based
on ambient weather conditions, primarily temperature. The model synthesizes data
from CAISO's [Prior Trade-Day Curtailment Reports](https://www.caiso.com/market/Pages/OutageManagement/CurtailedandNonOperationalGenerators.aspx)
and NCEI/NOAA's [Hourly Surface Weather](https://www.ncei.noaa.gov/data/global-hourly/)
dataset.

# Getting Started
The analysis process follows the following steps, each of which are performed by
the listed scripts:
1. Download CAISO Prior Trade Day Curtailment Reports - retrieve_caiso_curtailments.py
2. Download Weather Data - retrieve_weather.py
3. Assign weather stations to curtailed resources - pair_resources_to_weather_stations.py and select_weather_stations.py
4. Merge curtailment and weather data - model_curtailments.py
5. Find best-fit slopes for temperatures vs. curtailments - model_curtailments.py
6. Use slopes to find intercepts based on annual historic or forecast temperatures - forecast_curtailments.py
7. Apply slopes and intercepts to model derates for historic or forecast weather - forecast_curtailments.py
8. Analyze forecast derate results - derate_percentiles.py

# Explanation of Scripts
The model requires two datasets, both of which can be downloaded using Python
scripts. These scripts can be imported and instantiated as objects in other
scripts or run on their own, applying default parameters. Make sure the output
directories and filenames are adjusted to the target file system before running
in this mode.

## Download Curtailments
The `retrieve_caiso_curtailments.py` script contains a class to help
download one or multiple prior trade-day reports from CAISO's website. If run
as a standalone script, it will download all reports since June 18, 2021, when
CAISO first started publishing the reports in the current format online.

## Download Weather Data
The `retrieve_weather.py` script contains a class to help download hourly
weather data from selected weather stations from NCEI/NOAA's website. These
datasets are downloaded for entire years at once, so large file sizes are to be
expected. If run as a standalone script, it downloads data for twelve weather
stations for the years 2021-2024.

## Match Resources and Weather Stations
Once the two primary data sources are downloaded locally, they must be combined.
This step involves pairing each resource with a weather station and matching
each hourly weather observation with any curtailments. If desired, additional
hours where no curtailments are reported may be paired to weather observation,
thereby imputing zero-valued curtailments into the dataset. The
`pair_resources_to_weather_stations.py` script looks up the locations of each
resource and weather station from Energy Division's EZDB Postgres server and
calculates the distances between each pair and identifies the closest weather
station to each resource. The `select_weather_stations.py` script alternatively
selects a given number of weather stations based on their proximity to multiple
resources, and these may be input into the sql string in the
`identify_weather_stations()` function.

## Determine Derate Model Parameters
Once each resource is assigned a weather station, the
`model_curtailments.py`script may be run. This script creates an object
with methods to subdivide resource curtailments into hourly intervals and merges
with weather data from the resource's selected weather station. The combined
data is then saved to file which can be reused on later analyses by setting the
`use_processed` property to True, to avoid having to re-combine the data every
time the script is run.

The `model_curtailments.py` script then performs a series of linear
regression analyses based on selected parameters. There are two methods for
performing two versions of the ambient temperature derate analysis: `regress()`
and `regress_multilinear()`. In the first method, regression is performed on
curtailment percentage versus temperature for each resources, then resources are
filtered based on the goodness of their best-fit lines, temperatures are
normalized such that each resource's best-fit lines intersect at a given
curtailment level, and finally, a second regression analysis is performed on all
curtailments and normalized temperatures within each unit type to determine the
slope of the overall best-fit line in the group. In second method, the
curtailment and weather data set is appended with boolean variables to indicate
unit type and weather station, and the additional variables allow the best-fit
line to float with varying offsets for each weather station to determine the
overall best slope for all resources within a group. The multilinear analysis
allows more resources to be considered, providing more characteristic results
for the full dataset. In either case, the analysis produces parameters to
define the piecewise-linear functions for each pair of weather station and unit
type.

## Apply Derate Model
Once the slopes of the best-fit lines are determined, they can be input into the
`forecast_derates.py` script, which creates an object with methods to calculate
the derate intercepts and then calculate derates for each class consisting of a
resource unit type and weather station. Rather than using the .csv files
downloaded from the NOAA/NCEI website as with earlier scripts, this script reads
.parquet files generated by Energy Division staff's Climate-Informed Forecasting
analysis. The .parquet files are compressed binary files containing either
historical or projected temperatures at locations coincident to the weather
stations used in the regression analysis.

## Analyze Results
The `derate_percentile.py` script reads a specified set of forecast derates and
evaluates requested derate percentiles for each unit type and weather station.