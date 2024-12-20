function [dataset, headers] = A3dDatasetParsing(filePath, A3dFieldNames, footprintPk)
% The function parses 3d georeferenced attributes (that are profiles with a
% vertical dimension) that are specified in A3dFieldNames
% footPrintPk defines where and when they are in a planar look, which can
% be combined with the vertical-bin field to form a complete primary key
% for dataset
% The uniqueness condition may be checked when the functionality is
% implemented

% first, find the size of the vertical dimension
A3dFieldNames = unique(A3dFieldNames, 'stable');
headers = [footprintPk, "bin_number", A3dFieldNames];
headers = replace(headers, "-", "_");
S = hdfinfo(filePath, "eos");
A3dFieldNamesNo = 1;
sampleData = hdfread(S.Swath, "Fields", A3dFieldNames{A3dFieldNamesNo});
sampleFieldDim = size(sampleData);
verticalBinsCt = sampleFieldDim(2);
footprintRecordCt = sampleFieldDim(1);
while verticalBinsCt == 1 || footprintRecordCt == 1
    A3dFieldNamesNo = A3dFieldNamesNo + 1;
    sampleData = hdfread(S.Swath, "Fields", A3dFieldNames{A3dFieldNamesNo});
    sampleFieldDim = size(sampleData);
    verticalBinsCt = sampleFieldDim(2);
    footprintRecordCt = sampleFieldDim(1);
end
footprintPksCt = numel(footprintPk);
A3dFieldNamesCt = numel(A3dFieldNames);
dataset = zeros(footprintRecordCt * verticalBinsCt, footprintPksCt + A3dFieldNamesCt + 1);

if verticalBinsCt > 1
    planarPk = [];
    % get the incomplete planar pk of the footprint
    for pkNo = 1: numel(footprintPk)
        pkFieldName = footprintPk(pkNo);
        data = hdfread(S.Swath, "Fields", pkFieldName);
        if size(data, 2) == 1
        % the attribute has only one value for each oribit
        % replicate them by recordsCt
        data = data * ones(1, footprintRecordCt);
        end
        planarPk = [planarPk, double(data)'];
    end
    
    % expand the incomplete planarPk dataset to include the pk and
    % specified 3d georeferenced attributes
    errmsg = sprintf("Start loading 3d-georeferenced data, %d footprints in the orbit in total...\n", footprintRecordCt);
    fprintf(1, errmsg);
    for fieldNo = 1: numel(A3dFieldNames)
        fieldName = A3dFieldNames(fieldNo);
        data = hdfread(S.Swath, "Fields", fieldName);
        for recordNo = 1: footprintRecordCt
            % get the planar pk (x, y, t) of the footprint pixel
            thisPlanarPk = planarPk(recordNo, :);

            % get the information for each vertical bin under the footprint
            for binNo = 1: verticalBinsCt
                dataTupleLineNo = (recordNo - 1) * verticalBinsCt + binNo;
                dataset(dataTupleLineNo, 1: footprintPksCt) = thisPlanarPk;
                dataset(dataTupleLineNo, footprintPksCt + 1) = binNo;
                dataset(dataTupleLineNo, footprintPksCt + fieldNo + 1) = data(recordNo, binNo);
            end
            if rem(recordNo, 6000) == 0 || recordNo == footprintRecordCt 
                errmsg = sprintf("%d/%d footprints for %d/%d attributes in the orbit have been processed...\n", recordNo, footprintRecordCt, fieldNo, A3dFieldNamesCt);
                fprintf(1, errmsg);
            end

        end

    end
end

end
