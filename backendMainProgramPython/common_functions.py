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