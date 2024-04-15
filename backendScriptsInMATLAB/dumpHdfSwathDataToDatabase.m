function success = dumpHdfSwathDataToDatabase(folderPath, fieldNames, footprintPks, databasePath)
tic;
[A2dDataset, A2dDatasetHeader, A3dDataset, A3dDatasetHeader] = readABatchOfHdfSwathData(folderPath, fieldNames, footprintPks);
endTime = toc;
disp(endTime);

% build the database if it does not exist
if ~exist(databasePath, 'file')
    % File does not exist, so create a 0 byte file
    fileId = fopen(databasePath, 'w');
    fclose(fileId);
end



saveDataset2Database(A3dDataset, A3dDatasetHeader, "test3d", databasePath);
saveDataset2Database(A2dDataset, A2dDatasetHeader, "test2d", databasePath);

success = 1;

end
