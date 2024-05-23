# this module handles all the source-server-local-server connection functionalities
import os

import paramiko
import sys
import stat
import common_functions

class sftpHandle:
    def __init__(self, host = "www.cloudsat.cira.colostate.edu", username = "d376liATuwaterloo.ca", password = ""):
        # create ssh client
        ssh_client = paramiko.SSHClient()

        # remote server credentials
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=host, username=username)

        ftpServerFiles = ssh_client.open_sftp()

        print("Successfully connected to the sftp source server.")

        rootDir = ftpServerFiles.getcwd()
        files = ftpServerFiles.listdir()



        self.sftpServer = ftpServerFiles
        self.sshClient = ssh_client

    def close(self):
        '''
        Never forget to close the connection at the end!!!
        '''
        self.sftpServer.close()
        self.sshClient.close()

    def isDir(self, contentPath):
        '''
        Check if contentPath on the source server is a directory
        :param contentPath: str
        :return: bool
        '''
        return stat.S_ISDIR(self.sftpServer.stat(contentPath).st_mode)


    def searchSubDir(self, folderName, sourceServerWD = '/', maxFolderNum = 1):
        '''
        sourceServerWD: The working directory from which we start the searching
        No more than 3 folders will be returned
        :return: List of Paths to the folder whose name contains folderName
        '''
        dirContents = self.sftpServer.listdir(sourceServerWD)
        subDirList = []  # contains all sub-directories containing folderName
        unsearchedDirs = []
        for dirContent in dirContents:
            dirContentPath = sourceServerWD + f"/{dirContent}"
            if self.isDir(dirContentPath):
                if folderName in dirContent:
                    subDirList.append(dirContentPath)
                    if len(subDirList) >= maxFolderNum:
                        return subDirList
                else:
                    unsearchedDirs.append(dirContentPath)

        for dirContentPath in unsearchedDirs:
            subDirList = subDirList + self.searchSubDir(folderName, dirContentPath)
            if len(subDirList) >= maxFolderNum:
                return subDirList

        return subDirList

    def fileList(self, dirOnSource, hardcopy = False, updateMode = False):
        '''
        Return a list of paths of all the files under dirOnSource

        Note: The search process is too time-consuming. Once we get one list for the root
        directory, it is usable until the server has a change. Hence, we will maintain a
        list in the server program root

        :param dirOnSource: str
        :return: List[str]
        '''
        trackRecordFileName = "sftpServerFilePath.txt"
        if not updateMode:
            # check if the track record file contains dirOnSource
            localFileList = os.listdir('./')
            if trackRecordFileName in localFileList:
                with open(f'./{trackRecordFileName}', 'r') as trackRecordFile:
                    sourceServerFileList = trackRecordFile.readlines()
                    sourceServerFileList = list(map(lambda s: s.strip(), sourceServerFileList))
                filePathsUnderDirOnSource = list(filter(lambda s: dirOnSource in s, sourceServerFileList))
                if len(filePathsUnderDirOnSource) != 0:
                    return filePathsUnderDirOnSource
            else:
                with open(f'./{trackRecordFileName}', 'a') as file:
                    file.write('')
        else:
            with open(f'./{trackRecordFileName}', 'a') as file:
                file.write('')

        if not self.isDir(dirOnSource):
            print(f"Warning: {dirOnSource} is not a folder. Failed to generate the file list under it.", file=sys.stderr)
        fileList = []
        currentFolderContentNames = self.sftpServer.listdir(dirOnSource)
        currentFolderContentPaths = list(map(lambda s: dirOnSource + f'/{s}', currentFolderContentNames))
        for contentPath in currentFolderContentPaths:
            if not self.isDir(contentPath):
                fileList.append(contentPath)
                print(contentPath)
            else:
                fileList = fileList + self.fileList(contentPath, False)

        if hardcopy:
            with open(f'./{trackRecordFileName}', 'a') as file:
                for filePath in fileList:
                    file.write(filePath + '\n')

        return fileList
    def downloadFileFromSourceToLocal(self, filePathOnSource, folderPathOnLocal):
        '''
        Download file from the source server to the local server given the file path
        on the source server.
        Returns the size of the downloaded file or None if it fails
        :param filePathOnSource: str
        :param folderPathOnLocal: str
        :return: int
        '''
        # handle the wrong cases where filePathOnSource is not a file
        try:
            if self.isDir(filePathOnSource):
                print(f"Failed to download {filePathOnSource}. It's a folder not a file.", file=sys.stderr)
                return None
        except Exception as e:
            print(f"An exception {e} occurred. Failed to download {filePathOnSource}.", file=sys.stderr)
            return None

        fileName = filePathOnSource.split('/')
        fileName = fileName[-1]
        localFilePath = folderPathOnLocal + f'/{fileName}'
        self.sftpServer.get(filePathOnSource, localFilePath)
        return common_functions.commonFunctions.get_file_size(localFilePath)


'''
obj = sftpHandle()
print(obj.sftpServer.listdir('//Data/2B-GEOPROF.P1_R05'))
obj.close()


obj = sftpHandle()
print(obj.downloadFileFromSourceToLocal("//Data/2B-GEOPROF.P1_R05/2012/078/2012078071622_31327_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf", "../example"))
obj.close()

obj = sftpHandle()
obj.fileList("//Data/1B-CPR.P_R05", True)
obj.close()


downloadFiles = [
        "//Data/2B-GEOPROF.P1_R05/2012/034/2012034161842_30691_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/034/2012034225347_30695_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/034/2012034143956_30690_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/034/2012034112223_30688_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/034/2012034193614_30693_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035102511_30702_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035021119_30697_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035201748_30708_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035170015_30706_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035003233_30696_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035035005_30698_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035134243_30704_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035120357_30703_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035183901_30707_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035152129_30705_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035070738_30700_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf",
        "//Data/2B-GEOPROF.P1_R05/2012/035/2012035233520_30710_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf"
    ]
for file in downloadFiles:
    fileName = file.split('/')[-1]
    sftpHandle().downloadFileFromSourceToLocal(file, f'/Users/liding/Documents/Documents/uwaterloo/research/CloudSat/temp')
exit(0)
'''

