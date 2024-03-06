function hdfPaths = findHDFFiles(folderPath)
    % Initialize an empty cell array to store HDF file paths
    hdfPaths = {};

    % Get the list of contents in the current folder
    fileList = dir(folderPath);
    
    % Iterate through the list of contents
    for i = 1:numel(fileList)
        % Construct the full path of the current item
        currentItemPath = fullfile(folderPath, fileList(i).name);
        
        % Check if the current item is a directory and not '.' or '..'
        if fileList(i).isdir && ~strcmp(fileList(i).name, '.') && ~strcmp(fileList(i).name, '..')
            % Recursively call findHDFFiles on subfolders
            subFolderHDFPaths = findHDFFiles(currentItemPath);
            % Append the HDF file paths found in the subfolder to hdfPaths
            hdfPaths = [hdfPaths; subFolderHDFPaths];
        elseif ~fileList(i).isdir
            % Check if the current item is an HDF file
            [~, ~, fileExt] = fileparts(currentItemPath);
            if strcmpi(fileExt, '.hdf')
                % Add the current HDF file path to hdfPaths
                hdfPaths = [hdfPaths; {currentItemPath}];
            end
        end
    end
end

