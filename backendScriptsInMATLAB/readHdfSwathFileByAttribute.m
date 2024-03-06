function [data, S] = readHdfSwathFileByAttribute(filePath, fieldName)
S = hdfinfo(filePath, "eos");
data = hdfread(S.Swath,"Fields", fieldName);
end