tic;


A2dFieldNames = ["Profile_time", "Data_quality"];
footprintPks = ["TAI_start", "Profile_time"];
folderPath = "/Users/leo.li27/.ssh/2020/012";
filePaths = ["/Users/liding/Documents/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/dataThroughPutRecords/0001/2012035070738_30700_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf", "/Users/liding/Documents/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/dataThroughPutRecords/0001/2012035233520_30710_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf"];
% folderPath = '/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/tempDataFolder/0000';
fieldNames = ["Latitude", "Longitude", "UTC_start", "Height", "Data_quality", "Data_status", "Data_targetID", "RayStatus_validity", "SurfaceHeightBin", "SurfaceHeightBin_fraction", "Gaseous_Attenuation", "Sigma-Zero", "MODIS_Cloud_Fraction", "CPR_Echo_Top", "sem_NoiseFloor", "sem_NoiseFloorVar", "sem_NoiseGate", "sem_MDSignal", "Radar_Reflectivity", "MODIS_cloud_flag", 'DEM_elevation', 'CPR_Cloud_mask', 'Clutter_reduction_flag', 'Vertical_binsize', 'Pitch_offset', 'Roll_offset', 'Navigation_land_sea_flag', 'MODIS_Cloud_Fraction', 'MODIS_scene_char', 'MODIS_scene_var'];
dumpHdfSwathDataToDatabase(filePaths, fieldNames, footprintPks, '../db.sqlite3');

[A2dDataset, A2dDatasetHeader, A3dDataset, A3dDatasetHeader] = readABatchOfHdfSwathData(filePaths, fieldNames, footprintPks);
endTime = toc;
disp(endTime);
% dataArray = readABatchOfHdfSwathData("/Users/leo.li27/.ssh/2020", ["Longitude", "Latitude", "Radar_Reflectivity"]);

dataset = A3dDatasetParsing(filePath, A3dFieldNames, footprintPk);


