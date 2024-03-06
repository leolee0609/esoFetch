# the module integrates all the data management functionalities
import os
import sys
import shutil
import sftpHandle
import common_functions

class CloudSatDataManager:
    '''
    A CloudSatDataManager object contains a SFTP connection to the CloudSat DPC server and
    should be attached with a temp data folder for data throughput and analysis on our
    local server

    tempDataDir: Path to the data that you want to use for throughput data
    tempDataSizeWindow: Maximum amount of content stored in tempDataDir in bytes
    1GB = 1024 * 1024 * 1024 by default
    '''
    def __init__(self, tempDataDir = '../tempDataFolder', tempDataSizeWindow = 1024 * 1024 * 1024):
        self.tempDataDir = tempDataDir
        self.tempDataSizeWindow = tempDataSizeWindow

    def getABatchOfData(self, jobId, productName):
        '''
        Download data from the source server to the local server tempDataDir folder until
        we reach tempDataSizeWindow or we have passed all the product files on the
        source server

        We maintain a file that tracks the progress discretely in the tempDataDir
        The file is named as "requestUserIP_hisRequestNo.txt"
        The protool of the record file is:
        productName
        productFolderLocationOnSourceServer
        DownloadedHdfFiles

        productName: One of {"2B-GEOPROF.P1_R05", ...}
        '''
        # connect to the source server
        sourceServer = sftpHandle.sftpHandle()
        # data space for the jobId
        dataSpacePath = self.tempDataDir + "/{jobId}".format(jobId=jobId)
        # check if the record file has been built
        recordFileName = str(jobId) + '.txt'
        recordFilePath = self.tempDataDir + f"/{recordFileName}"
        productDirOnSource = ''

        # check if the download task is triggered first name without any records
        if recordFileName not in os.listdir(self.tempDataDir):
            # it's the first-time trigger, initialize the plan of the download task
            # and build a record file for it

            # locate the requested product on the source machine
            productDirOnSource = sourceServer.searchSubDir(productName)
            if len(productDirOnSource) == 0:
                print(f"Failed to find {productName} on CloudSat server. Exit.", sys.stderr)
                sourceServer.close()
                return False
            if len(productDirOnSource) > 1:
                print(f"More than 1 folders on CloudSat server are found to contain the requested product: {productDirOnSource}. The first one is selected.", file=sys.stderr)
            productDirOnSource = productDirOnSource[0]

            # build a track record for the job
            with open(recordFilePath, 'w') as recordFile:
                recordFile.write(productName + '\n')
                recordFile.write(productDirOnSource + "\n")

            # build the folder for the job that stores the data
            os.makedirs(dataSpacePath)

        with open(recordFilePath, 'r') as recordFile:
            recordContent = recordFile.readlines()
        recordContent = list(map(lambda s: s.strip(), recordContent))
        # the second line is the product's directory path in the server
        recordContent.pop(0)
        if recordContent != productName:
            print(f"Warning, the record for {jobId} is not consistent with the input productName {productName}", file=sys.stderr)
        productDirOnSource = recordContent.pop(0)
        # get all the files under productDirOnSource
        fileListToBeDownloaded = sourceServer.fileList(productDirOnSource)

        # rest of the contents are all downloaded hdf files in the folder
        # clear the data space
        shutil.rmtree(dataSpacePath)
        os.makedirs(dataSpacePath)
        # get the downloaded hdf files
        downloadedHdfFiles = list(map(lambda s: s.strip(), recordContent))
        # filter out all the files that has once been downloaded
        fileListToBeDownloaded = list(filter(lambda s: s not in downloadedHdfFiles, fileListToBeDownloaded))

        # fetch the files in productDirOnSource on the source server to dataSpace
        dataSpacePathDataSize = 0
        dataSpacePathDataRealSize = common_functions.commonFunctions.get_folder_size(dataSpacePath)
        if dataSpacePathDataSize != dataSpacePathDataRealSize:
            print(f"Warning, the temp data dir is not cleared. The size is {dataSpacePathDataRealSize} now.", file=sys.stderr)
        while dataSpacePathDataSize < self.tempDataSizeWindow:
            filePathOnSource = fileListToBeDownloaded.pop(0)
            dataSpacePathDataSize += sourceServer.downloadFileFromSourceToLocal(filePathOnSource, dataSpacePath)
            downloadedHdfFiles.append(filePathOnSource)
            print(f"Successful download: {filePathOnSource}. {dataSpacePathDataSize/(1024 * 1024)} MB of things downloaded.", file=sys.stderr)

        # update the track record for the job id
        with open(recordFilePath, 'w') as recordFile:
            recordFile.write(productName + '\n')
            recordFile.write(productDirOnSource + "\n")
            for hdfFileName in downloadedHdfFiles:
                recordFile.write(hdfFileName + "\n")

        # close the server
        sourceServer.close()
        return True


tempDataDir = "/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/tempDataFolder"
CSDM = CloudSatDataManager(tempDataDir)
print(CSDM.getABatchOfData("0000", "1B-CPR.P_R05"))

