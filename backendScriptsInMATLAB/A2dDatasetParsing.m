function [dataset, header] = A2dDatasetParsing(filePath, A2dFieldNames, footprintPk)
% The function returns a dataset with primary key (x, y, z) containing
% horizontal 2d-referenced attributes without vertical profiles
A2dFieldNames = unique(A2dFieldNames, 'stable');
header = [footprintPk, A2dFieldNames];
header = replace(header, "-", "_");
A2dFieldNamesCt = numel(A2dFieldNames);
footprintPkCt = numel(footprintPk);

recordsCt = 1;
footprintPkNo = 0;
while recordsCt == 1
    footprintPkNo = footprintPkNo + 1;
    S = hdfinfo(filePath, "eos");
    sampleData = hdfread(S.Swath, "Fields", footprintPk{footprintPkNo});
    dataDim = size(sampleData);
    recordsCt = dataDim(2);
end

dataset = zeros(recordsCt, footprintPkCt + A2dFieldNamesCt);

for pkNo = 1: footprintPkCt
    pkName = footprintPk(pkNo);
    pkRecords = hdfread(S.Swath, "Fields", pkName);
    if size(pkRecords, 2) == 1
        % the attribute has only one value for each oribit
        % replicate them by recordsCt
        pkRecords = pkRecords * ones(1, recordsCt);
    end
    dataset(:, pkNo) = pkRecords;
end

for fieldNo = 1: A2dFieldNamesCt
    fieldName = A2dFieldNames(fieldNo);
    data = hdfread(S.Swath, "Fields", fieldName);
    dataset(:, footprintPkCt + fieldNo) = data;
end
end
