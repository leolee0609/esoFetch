function success = dumpHdfSwathDataToDatabase(filePaths, fieldNames, footprintPks, databasePath)
tic;
[A2dDataset, A2dDatasetHeader, A3dDataset, A3dDatasetHeader] = readABatchOfHdfSwathData(filePaths, fieldNames, footprintPks);
endTime = toc;
disp(endTime);

% build the database if it does not exist
if ~exist(databasePath, 'file')
    % File does not exist, so create a 0 byte file
    fileId = fopen(databasePath, 'w');
    fclose(fileId);
end

resolutionVolumePks = [footprintPks, "bin_number"];

saveDataset2Database(A3dDataset, A3dDatasetHeader, "test3d", databasePath, resolutionVolumePks);
saveDataset2Database(A2dDataset, A2dDatasetHeader, "test2d", databasePath, footprintPks);

success = 1;

end
