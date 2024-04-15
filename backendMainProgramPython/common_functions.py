import os
class commonFunctions:
    def get_folder_size(folder_path):
        '''
        Return the size of a folder in bytes
        :return: int
        '''
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        return total_size

    def get_file_size(file_path):
        '''
        Return the size of a file in bytes
        :return: int
        '''
        return os.path.getsize(file_path)

    def create_where_clause(query_dict):
        '''
        :param filterCriteria: Dict{field: (oneOf{'2d', '3d', ''}, operator, threshold)},
        where '2d' means 2d-georeferenced dataset only, and '3d' means 3d-georeferenced dataset only,
        '' or no such tuple value (the key is a tuple of length 2 rather than 3) means that
        the field can be both. Example: {'Cloud_mask': (lambda x, y: x > y), 5} means that we want to select out
        all the data records whose Cloud_mask field value are larger than 5. Note that the operator must be BINARY.
        The filter criteria dictionary is passed to the data filter function as a parameter
        :return:
        '''
        where_clauses = []
        params = []
        for field, (operator, threshold) in query_dict.items():
            # We add a placeholder for the value, but not the field or operator
            where_clause = f"{field} {operator} ?"
            where_clauses.append(where_clause)
            params.append(threshold)
        return ' AND '.join(where_clauses), params