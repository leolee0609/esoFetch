# the module implements processing parsed CloudSat data functionality including filtering, output reformatting, etc.
import sqlite3
import pandas as pd
import common_functions

class parsedDataProcessor:
    def __init__(self, dbFilePath):
        '''
        Input one database path to generate a processor object.
        :param dbFilePath: str
        '''
        self.dbFilePath = dbFilePath


    def dataFilterDF(self, filterCriteria = None, outputColumns = ['*']):
        '''
        The function sets a filter to select all the records in database that
        satisfy filterCriteria. It then outputs a filtered dataset with only outputColumns
        :param filterCriteria: Dict{field: (oneOf{'2d', '3d', ''}, operator, threshold)},
        where '2d' means 2d-georeferenced dataset only, and '3d' means 3d-georeferenced dataset only,
        '' or no such tuple value (the key is a tuple of length 2 rather than 3) means that
        the field can be both. Example: {'Cloud_mask': (lambda x, y: x > y), 5} means that we want to select out
        all the data records whose Cloud_mask field value are larger than 5. Note that the operator must be BINARY.
        The filter criteria dictionary is passed to the data filter function as a parameter
        :param outputColumns: List[column_names: str]
        :return: DataFrame
        '''
        conn = sqlite3.connect(self.dbFilePath)
        where_clause, params = common_functions.commonFunctions.create_where_clause(filterCriteria)
        sqlFields = ','.join(outputColumns)
        # check if there exists 3d attributes
        # 1 is 2d only, 2 is 3d only, 3 is both existing
        status = -1
        attributeIndicatorList = list(map(lambda tup: tup[1][0], filterCriteria.items()))
        if len(set(attributeIndicatorList)) == 1:
            if attributeIndicatorList[0] == '2d':
                status = 1
            elif attributeIndicatorList[0] == '3d':
                status = 2
        else:
            status = 3

        if status == 1:
            table_name = 'test2d'
        elif status == 2:
            table_name = 'test3d'
        else:
            table_name = 'test3d JOIN test2d ON test3d.Latitude = test2d.Latitude AND test3d.Longitude = test2d.Longitude AND test3d.UTC_start = test2d.UTC_start AND test3d.Profile_time = test2d.Profile_time'

        sqlQuery = f"SELECT {sqlFields} FROM {table_name} WHERE {where_clause}"
        print(f'Executing SQL query: {sqlQuery}...')
        # Execute the query and fetch the result into a pandas DataFrame
        dataframe = pd.read_sql_query(sqlQuery, conn, params=params)

        # Close the database connection
        conn.close()

        return dataframe


'''
DP = parsedDataProcessor('../db.sqlite3')
filterCriteria = {'Height': ('<=', 0)}
DF = DP.dataFilterDF(filterCriteria)
print(DF)
'''

