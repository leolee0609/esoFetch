tic;
filePath = "/Users/leo.li27/.ssh/2020/008/2020008031245_72964_CS_2B-GEOPROF_GRANULE_P1_R05_E09_F00.hdf";
[temp, info] = readHdfSwathFileByAttribute(filePath, "Longitude");

A2dFieldNames = ["Profile_time", "Data_quality"];
footprintPks = ["Latitude", "Longitude"];
folderPath = "/Users/leo.li27/.ssh/2020/008";
fieldNames = ["Data_quality", "Radar_Reflectivity", "MODIS_cloud_flag", "Sigma-Zero"];

[A2dDataset, A3dDataset] = readABatchOfHdfSwathData(folderPath, fieldNames, footprintPks);
endTime = toc;
disp(endTime);
%dataArray = readABatchOfHdfSwathData("/Users/leo.li27/.ssh/2020", ["Longitude", "Latitude", "Radar_Reflectivity"]);

dataset = A3dDatasetParsing(filePath, A3dFieldNames, footprintPk);
