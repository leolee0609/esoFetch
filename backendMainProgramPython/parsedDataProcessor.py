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

    def _determine_status(self, filterCriteria):
        '''
        :return: Oneof(1, 2, 3)
        1: 2d attributes only;
        2: 3d attributes only;
        3: Mixed
        '''
        attributeIndicatorList = [tup[1][0] for tup in filterCriteria.items()]
        if len(set(attributeIndicatorList)) == 1:
            if attributeIndicatorList[0] == '2d':
                return 1
            elif attributeIndicatorList[0] == '3d':
                return 2
        return 3

    def _select_table(self, status):
        if status == 1:
            return 'test2d'
        elif status == 2:
            return 'test3d'
        else:
            return 'test3d JOIN test2d ON test3d.Latitude = test2d.Latitude AND test3d.Longitude = test2d.Longitude AND test3d.UTC_start = test2d.UTC_start AND test3d.Profile_time = test2d.Profile_time'

    def dataFilterDF(self, filterCriteria=None, outputColumns=['*'], applySQL2DB=False, temp_table_name='query_result'):
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
        :param applySQL2DB: bool If True, apply the filter directly to the database.
        :return: DataFrame The filtered dataset.
        '''
        conn = sqlite3.connect(self.dbFilePath)
        where_clause, params = common_functions.commonFunctions.create_where_clause(filterCriteria)
        sqlFields = ','.join(outputColumns)
        status = self._determine_status(filterCriteria)

        table_name = self._select_table(status)

        sqlQuery = f"SELECT {sqlFields} FROM {table_name} WHERE {where_clause}"
        print(f'Executing SQL query: {sqlQuery}...')

        dataframe = pd.read_sql_query(sqlQuery, conn, params=params)

        if applySQL2DB:
            # Apply the changes to the database by creating a new table and replacing the old one
            create_temp_table_sql = f"CREATE TABLE {temp_table_name} AS {sqlQuery}"
            conn.execute("DROP TABLE IF EXISTS " + temp_table_name)
            conn.execute(create_temp_table_sql)
            conn.execute(f"DROP TABLE {table_name}")
            conn.execute(f"ALTER TABLE {temp_table_name} RENAME TO {table_name}")
            conn.commit()
            print(f"Changes applied to the database. The {table_name} table now contains only the filtered results.")

        conn.close()
        return dataframe


'''
DP = parsedDataProcessor('../db.sqlite3')
filterCriteria = {'Height': ('<=', 0)}
DF = DP.dataFilterDF(filterCriteria)
print(DF)
'''

