function saveDataset2Database(dataset, headers, tablename, dbPath)
% The function stores dataset with header to database whose path is dbPath

% Create a connection to the SQLite database
conn = sqlite(dbPath, 'connect');

% Create a table in the database with combined primary key
dataTable = array2table(dataset, 'VariableNames', headers);
% Insert data into the table
sqlwrite(conn,tablename,dataTable)
% Close the connection
close(conn);



end