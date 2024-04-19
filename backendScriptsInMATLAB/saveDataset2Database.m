function saveDataset2Database(dataset, headers, tablename, dbPath, primaryKey)
% Enhanced function to store dataset with headers into a SQLite database, including dynamic schema creation and primary key definition.

% Check if database file exists, if not, print a warning and initialize a new database
if exist(dbPath, 'file') == 0
    warning('Database does not exist. Creating new database.');
    % Create a connection to the SQLite database
    conn = sqlite(dbPath, 'create');
end

conn = sqlite(dbPath, "connect");


% Replace illegal characters in headers and primaryKey
headers = strrep(headers, '-', '_');
primaryKey = strrep(primaryKey, '-', '_');

% Determine the data types for each column in the dataset
dataTypes = determineDataTypes(dataset);

% Construct the SQL CREATE TABLE command with appropriate data types and primary key
createTableSQL = ['CREATE TABLE IF NOT EXISTS ' tablename ' ('];
for i = 1:length(headers)
    createTableSQL = [createTableSQL headers{i} ' ' dataTypes{i}];
    if ismember(headers{i}, primaryKey)
        createTableSQL = [createTableSQL ' PRIMARY KEY'];
    end
    if i == length(headers)
        createTableSQL = [createTableSQL ');'];
    else
        createTableSQL = [createTableSQL ', '];
    end
end

% Check if the table exists
tableExistsQuery = sprintf('SELECT name FROM sqlite_master WHERE type="table" AND name="%s";', tablename);
tableExistsResult = fetch(conn, tableExistsQuery);
if isempty(tableExistsResult)
    warning(sprintf('Table %s does not exist. Creating table.', tablename));

    % Initialize the SQL CREATE TABLE command
    createTableSQL = sprintf('CREATE TABLE %s (', tablename);

    % Append each column definition to the SQL command
    for i = 1:length(headers)
        createTableSQL = sprintf('%s %s %s', createTableSQL, headers{i}, dataTypes{i});
        if i < length(headers)
            createTableSQL = sprintf('%s, ', createTableSQL);
        end
    end

    % Add primary key for composite key scenario
    if ~isempty(primaryKey)
        pkString = strjoin(primaryKey, ', ');
        createTableSQL = sprintf('%s, PRIMARY KEY(%s));', createTableSQL, pkString);
    else
        createTableSQL = sprintf('%s);', createTableSQL);
    end

    % Execute the creation of the table
    exec(conn, createTableSQL);
end

% Create a table using the dataset schema and insert data
dataTable = array2table(dataset, 'VariableNames', headers);
sqlwrite(conn, tablename, dataTable);

% Close the connection
close(conn);

end

function dataTypes = determineDataTypes(dataset)
    dataTypes = cell(1, size(dataset, 2));
    for i = 1:size(dataset, 2)
        colData = dataset(:, i);
        if iscell(colData)
            % Handle cell array column data: check data types within cells
            if all(cellfun(@isnumeric, colData))
                dataTypes{i} = 'REAL';
            elseif all(cellfun(@islogical, colData))
                dataTypes{i} = 'INTEGER';
            else
                dataTypes{i} = 'TEXT';
            end
        else
            % Handle numeric/logical array column data directly
            if islogical(colData)
                dataTypes{i} = 'INTEGER'; % SQLite uses INTEGER for booleans
            elseif isnumeric(colData)
                dataTypes{i} = 'REAL';
            else
                dataTypes{i} = 'TEXT'; % Default to TEXT for unsupported types
            end
        end
    end
end
