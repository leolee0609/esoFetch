function [A2dDataSet, A2dDatasetHeader, A3dDataSet, A3dDatasetHeader] = readABatchOfHdfSwathData(folderPath, fieldNames, footprintPks)
% the function reads all the hdf-eos data in folderPath and returns the
% 2 datasets: The first one with 2d geolocation as the primary key,
% recording the 2d attributes without the vertical dimension; Whereas the
% second dataset records 3d georeferenced attributes
% NOTE: You MUST specify x, y, z geolocation fields in A3dGeoFieldNames,
% which would be used as the primary keys

% first, find all hdf files under folderPath
filePaths = findHDFFiles(folderPath);
disp('List of file paths:');
disp(filePaths);

A2dFieldNames = []; % attributes georeferenced in 2d
A3dFieldNames = []; % attributes georeferenced in 3d

sampleS = hdfinfo(filePaths{1}, "eos");

recordsCt = 0;
% split the fieldNames into 2d georeferenced and 3d georeferenced
for fieldNo = 1: numel(fieldNames)
    fieldName = fieldNames(fieldNo);
    sampleData = hdfread(sampleS.Swath, "Fields", fieldName);
    fieldShape = size(sampleData);
    fieldVerticalBinsCt = fieldShape(1);
    recordsCt = fieldShape(2);
    if fieldVerticalBinsCt == 1
        A2dFieldNames = [A2dFieldNames, fieldName];
    else
        A3dFieldNames = [A3dFieldNames, fieldName];
    end
end
A3dFieldNames = unique(A3dFieldNames, 'stable');
A2dFieldNames = unique(A2dFieldNames, 'stable');
% parse the files and gather datasets
errmsg = sprintf("Start parsing a batch of data under %s, overall %d hdf files.", folderPath, numel(filePaths));
fprintf(1, errmsg);
A2dCumulativeRecordsCt = 0;
A3dCumulativeRecordsCt = 0;
A2dDataSet = zeros(numel(filePaths) * recordsCt * 2, numel(footprintPks) + numel(A2dFieldNames));
A3dDataSet = zeros(numel(filePaths) * recordsCt * 125 * 2, numel(footprintPks) + 1 + numel(A3dFieldNames));
A2dsubDatasetEndLineNo = 0;
A3dsubDatasetEndLineNo = 0;
for fileNo = 1: numel(filePaths)
    filePath = filePaths{fileNo};
    [A2dDataSubSet, A2dDatasetHeader] = A2dDatasetParsing(filePath, A2dFieldNames, footprintPks);
    [A3dDataSubSet, A3dDatasetHeader] = A3dDatasetParsing(filePath, A3dFieldNames, footprintPks);
    recordsCt = size(A2dDataSubSet, 1);
    A2dCumulativeRecordsCt = A2dCumulativeRecordsCt + recordsCt;
    A3dCumulativeRecordsCt = A3dCumulativeRecordsCt + 125 * recordsCt;

    A2dsubDatasetStartLineNo = A2dsubDatasetEndLineNo + 1;
    A2dsubDatasetEndLineNo = A2dsubDatasetStartLineNo + recordsCt - 1;
    A2dDataSet(A2dsubDatasetStartLineNo: A2dsubDatasetEndLineNo, :) = A2dDataSubSet;

    A3dsubDatasetStartLineNo = A3dsubDatasetEndLineNo + 1;
    A3dsubDatasetEndLineNo = A3dsubDatasetStartLineNo + recordsCt * 125 - 1;
    A3dDataSet(A3dsubDatasetStartLineNo: A3dsubDatasetEndLineNo, :) = A3dDataSubSet;

    errmsg = sprintf("%d/%d hdf files completed...", fileNo, numel(filePaths));
    fprintf(1, errmsg);
    
end

% remove the zero rows
idx = find(~all(A3dDataSet == 0, 2), 1, 'last');
A3dDataSet = A3dDataSet(1:idx, :);

idx = find(~all(A2dDataSet == 0, 2), 1, 'last');
A2dDataSet = A2dDataSet(1:idx, :);

A2dDataSetRecordsCt = size(A2dDataSet, 1);

% check the integrity of the data
if A2dDataSetRecordsCt ~= A2dCumulativeRecordsCt
    errmsg = sprintf("Warning, 2d dataset might lost some records from hdf files. %d records in dataset, but %d records in hdf file.", A2dDataSetRecordsCt, A2dCumulativeRecordsCt);
    fprintf(2, errmsg);
end
A3dDataSetRecordsCt = size(A3dDataSet, 1);
if size(A3dDataSet, 1) ~= A3dCumulativeRecordsCt
    errmsg = sprintf("Warning, 3d dataset might lost some records from hdf files. %d records in dataset, but %d records in hdf file.", A3dDataSetRecordsCt, A3dCumulativeRecordsCt);
    fprintf(2, errmsg);
end

% check if there are zero rows in the datasets
zeroRowsInA2dDataSetCt = sum(sum(A2dDataSet, 2) == 0);
zeroRowsInA3dDataSetCt = sum(sum(A3dDataSet, 2) == 0);
if zeroRowsInA2dDataSetCt ~= 0
    errmsg = sprintf("Warning, %d zero rows might exist in 2d dataset. The dataset might not be compact.", zeroRowsInA2dDataSetCt);
    fprintf(2, errmsg);
end
if zeroRowsInA3dDataSetCt ~= 0
    errmsg = sprintf("Warning, %d zero rows might exist in 3d dataset. The dataset might not be compact.", zeroRowsInA3dDataSetCt);
    fprintf(2, errmsg);
end

end
