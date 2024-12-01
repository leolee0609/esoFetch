## Overview
The document describes our CloudSat data acquisition web service project. The purpose of the project is to make downloading specific CloudSat data more convenient and automatic.

**Note: The project is currently being repurposed to a broader usage. The information might become outdated soon.**

## CloudSat data
CloudSat is an active remote sensing (RS) satellite from NASA with a Cloud Profiling Radar (CPR). The CPR provides three-dimensional structure of clouds and other hydrometeors over the globe. Hence, besides horizontal coordinates (i.e., Latitude and Longitude), CloudSat data products have vertical profiles (e.g., vertical bin, height). The data processing center (DPC) provides a list of hierarchical data products, from 0B-level raw data to level3 application-level data. The product information is here: https://www.cloudsat.cira.colostate.edu/data-products#. We are interested in 1B to 2B data, which are all encoded in hdf-eos files More info about hdf-eos files: https://wiki.earthdata.nasa.gov/display/SDPSDOCS/HDF-EOS2+v3.0+Function+Reference+Guide. Each hdf-eos file contain all records within one orbit of data, starting from some point on the equator (or nearby). Each data product consists of three types of fields (i.e., attributes, or columns) for each observational record: 1) Orbit field, which is one-value-per-file; 2) Footprint field, which is one-value-per footprint (referenced by latitude, longitude, time); And 3) Resolution-volume field, which is one-value-per-resolution-volume (referenced by latitude, longitude, height, and time). For convenience, we call the later two types of fields 2d attributes and 3d attributes.
## Official data acquisition interface
CloudSat DPC offers an online data ordering system, which supports setting a geofence, but 1) it doesn't pop up all the available records along time that are within the geofence for us to select, just like USGS, 2) It only supports a maximum of a 2-month time window. You will have to try 6 times for exhausting just a year; 3) They never let you select data based on specific fields/attributes (e.g., CPR_Cloud_mask > 5; Radar_Reflectivity > 0); and 4) It's pretty hard to parse and do data analysis on hdf-eos files. The interface is here: https://www.cloudsat.cira.colostate.edu/order/. Besides, they open the SFTP access to the data server. The connection and operation tutorial is here: https://www.cloudsat.cira.colostate.edu/order/sftp-access. The SFTP access enables us to automatically search and download data using algorithms.
## Project conceptual design
We develop an online web service that throws back the CloudSat data that our user wants. The framework prototype is like a proxy, which connects the DPC source server and the user's browser. We call the DPC server the source server, and the server where we deploy the platform the local server.
The very brief workflow is that: 1) Get data requests from the user, where the user sets a filter of what data they want; 2) Set a unique job id and a set of track records for the job; 3) Start the job by constructing a list of files that we should download and parse from the source server; 4) Download and parse the local data and apply the filer that the user sets; 5) Throw back the data that the user wants; 6) Complete and delete the job
More details will be discussed in the below implementation section, where some parallelization and caching mechanisms are applied to optimize the workflow.
## Implementation
### Caching mechanism
Since we have very limited cloud disk storage and transmission resources, we must apply a caching mechanism to balance between time consumption and disk storage. By setting the maximum volume of space we can use for the service, we download and process the data batch-by-batch to maintain the disk storage use within the limitation dynamically.
### Parallelization
Many sub-tasks in the workflow can run in parallel to save time. e.g., Data downloading, parsing, and deleting.
### Data manager
The global manager of the whole service workflow is implemented in a Python CloudSatDataManager module. The module contains a class that is the brain of the service.
### Data parser
CloudSat data is parsed and dumped into database using a set of MATLAB functions. The only parent caller of the MATAB functions is the Python data manager.
