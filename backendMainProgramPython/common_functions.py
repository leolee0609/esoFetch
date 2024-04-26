import os
from datetime import datetime
import re
from datetime import datetime, timedelta

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

    def tai_to_datetime(tai_seconds, tai_epoch=datetime(1993, 1, 1)):
        """
        Convert TAI seconds since the epoch to a datetime object.
        """
        delta = timedelta(seconds=tai_seconds)
        return tai_epoch + delta

    def parse_tai_time(tai_input):
        """ Attempt to parse different TAI formats into a datetime object. """
        if isinstance(tai_input, datetime):
            return tai_input  # If it's already a datetime object, return it directly
        elif isinstance(tai_input, int):
            return commonFunctions.tai_to_datetime(tai_input)  # Assuming tai_input is in seconds since epoch
        elif isinstance(tai_input, str):
            try:
                return datetime.strptime(tai_input, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                raise ValueError("Invalid TAI string format, expected 'YYYY-MM-DDTHH:MM:SS'")
        else:
            raise TypeError("Date must be either datetime object, TAI seconds int, or TAI time string")

    def is_within_range(start, end, target):
        """ Check if target date is within the range from start to end, inclusive.
            Dates can be datetime objects, TAI time strings, or TAI seconds.
        """
        start = commonFunctions.parse_tai_time(start)
        end = commonFunctions.parse_tai_time(end)
        target = commonFunctions.parse_tai_time(target)


        return start <= target <= end

    def tai_to_file_path(tai_input):
        """
        Convert any TAI input to a file path segment 'year/xxx' where 'xxx' is the day of the year.

        # Example usage
        tai_input = '2022-01-17T00:00:00'
        file_path = commonFunctions.tai_to_file_path(tai_input)
        print("File path for the given TAI input:", file_path)

        This is useful for getting the file path on the source server for specific dates
        """
        date_time = commonFunctions.parse_tai_time(tai_input)
        year = date_time.year
        day_of_year = date_time.strftime('%j')  # %j gives the day of the year as a zero-padded decimal number
        return f"{year}/{day_of_year}"

    def extract_year_day_from_path(file_path):
        """
        Uses a regular expression to find the year and day of the year in a file path.

        Parameters:
        - file_path: The complete or partial file path containing a 'year/xxx' segment.

        Returns:
        - A tuple of (year, day_of_year) if found, raises ValueError otherwise.
        """
        match = re.search(r'(\d{4})/(\d{1,3})', file_path)
        if not match:
            raise ValueError("No 'year/day' segment found in the file path")

        year, day_of_year = int(match.group(1)), int(match.group(2))
        return year, day_of_year

    def file_path_to_tai(file_path):
        """
        Converts a file path segment containing 'year/xxx' back into a TAI time string.

        Parameters:
        - file_path: A string which may include a segment in the format 'year/xxx'.

        Returns:
        - A TAI time string in the format 'YYYY-MM-DDTHH:MM:SS'.
        """
        year, day_of_year = commonFunctions.extract_year_day_from_path(file_path)

        # Create a datetime object for the first day of the year
        first_day_of_year = datetime(year, 1, 1)

        # Adjust the datetime object to the correct day of the year
        correct_date = first_day_of_year + timedelta(days=day_of_year - 1)

        # Format to TAI time string
        return correct_date.strftime('%Y-%m-%dT00:00:00')


    def list_files_under_folder(folder_path):
        file_list = []
        # Walk through all subfolders and files in the folder
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Construct the file path
                file_path = os.path.join(root, file)
                # Append the file path to the list
                file_list.append(file_path)
        return file_list

    def delete_files(file_list):
        # Iterate over the list of file paths
        for file_path in file_list:
            try:
                # Remove the file
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

