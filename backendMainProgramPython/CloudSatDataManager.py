# the module integrates all the data management functionalities
import os
import sys
import shutil
import sftpHandle
import common_functions
import hdfDataParsing
import json
import parsedDataProcessor
import threading
import time


class CloudSatJobs:
    '''
    Each job is created with one query. It typically contains data downloading, downloaded data
    filtering, filtered data formattig, formatted data outputing.
    Jobs should be created and managed only by CloudSatDataManager.
    Each job should contain one unique id, working space directory, limited disk space use,
    a status (e.g., ready, running, completed, failed). The fields must be distributed by
    CloudSatDataManager.
    Query jobs:
        For query jobs, each job can only process on one data product. Multiple data products
        operations, like sql join (e.g., find all records of a data product based on another)
        is expected to be run on user's local environment, which is beyond the scope of the
        service.
    '''
    def __init__(self, jobId, workingDir, spaceLimit, trackRecordsPaths, dateRange, productName = '2B-GEOPROF.P1_R05', jobType = 'Query', fieldNames = ['*'], filterCriteria = None, logFile = None):
        '''

        :param jobId:
        :param workingDir:
        :param spaceLimit:
        :param trackRecordsPaths:
        :param productName:
        :param jobType:
        :param filterCriteria: {fieldName1: ('sql', binary_sql_operator, threshold),
        fieldName2: ('py_bool_function', py_bool_func(), (parameters))}
        :param logFile:
        '''
        self.jobId = jobId
        self.productName = productName
        self.dateRange = dateRange
        self.workingDir = workingDir
        self.spaceLimit = spaceLimit
        self.jobType = jobType
        self.fieldNames = fieldNames
        if logFile == None:
            logFile = f'./{jobId}_logs.txt'
        self.logFile = logFile

        logMsg = f'Successfully created {self.jobType} job: {self.jobId}.'
        self.saveJobLog(logMsg)
        # {'download_track_record': path2downloadedTrackRecord,
        # 'parsing_track_record': path2parseTrackRecord,
        # 'filter_track_record': path2filterTrackRecord}
        self.trackRecordsPaths = trackRecordsPaths
        self.hdfDataParser = hdfDataParsing.hdfDataParsing()
        self.filterCriteria = filterCriteria
        self.databasePath = f'{workingDir}/{jobId}.sqlite3'
        self.dataFilter = parsedDataProcessor.parsedDataProcessor(self.databasePath)

        # dataframe that is a thumb of the job for quick visualization
        self.jobThumb = None

    def parseTrackRecord(self, taskType = 'download'):
        trackRecordFile = None
        if 'download' in taskType:
            trackRecordFile = self.trackRecordsPaths['download_track_record']
            with open(trackRecordFile, 'r') as recordFile:
                trackRecordContent = json.load(recordFile)
            progress = trackRecordContent['progress']
            todoList = trackRecordContent['to_do_list']
            processedFiles = trackRecordContent['downloaded_files']
            return (progress, todoList, processedFiles)
        elif 'pars' in taskType:
            trackRecordFile = self.trackRecordsPaths['parsing_track_record']
            with open(trackRecordFile, 'r') as recordFile:
                trackRecordContent = json.load(recordFile)

            progress = trackRecordContent['progress']
            todoList = trackRecordContent['to_do_list']
            processedFiles = trackRecordContent['parsed']
            return (progress, todoList, processedFiles)



    def __str__(self):
        '''
        Overload print() function for jobs to get the progress and status
        :return: str
        '''
        # unpack download progress
        downloadTrackRecordFile = self.trackRecordsPaths['download_track_record']
        prtmsg = f'{self.jobType} job ID: {self.jobId}, job with parallel threads:'
        downloadProgress, unDownloadedFiles, downloadedFiles = self.parseTrackRecord('download')
        unDownloadedFilesCt = len(unDownloadedFiles)
        downloadedFilesCt = len(downloadedFiles)
        overallFiles = unDownloadedFilesCt + downloadedFilesCt
        parsedProgress, unparsedFiles, parsedFiles = self.parseTrackRecord('parse')
        prtmsg = (prtmsg + f'\nDownload task progress: {downloadProgress}, {unDownloadedFilesCt}/{overallFiles};\n'
                           + f'Parsing task progress: {parsedProgress}, {unparsedFiles}/{overallFiles}.')
        return prtmsg

    def saveJobLog(self, info, err = False, timeStamp = True):
        '''
        Save info to
        :param str:
        :return: str
        '''
        if info[-1:] != '\n':
            # if there is no line breakers, append one to the end of the log message
            info = info + '\n'
        if timeStamp:
            # add a time stamp
            info = str(time.time()) + ': ' + info
        if err:
            printedErrMsg = f'From a {self.jobType} job {self.jobId}:\n'
            print(printedErrMsg + info, sys.stderr)
        with open(self.logFile, 'a') as file:
            print(info, file=file)
        return info

    def targetFileListOnSourceServer(self):
        '''
        Generate a list of file paths that may contain records of interest on the source server
        Will be hard copied into a file at a track record for reuse and job tracking/retrying
        The function is at least responsible for doing a first filtering based on date
        Track record protocol format: json{'progress': int % Completed, 'to_do_list': List[filePaths],
        'downloaded_files': List[filePaths]}
        Theoretically, downloaded_files should be a subset of to_do_list, and #to_do_list/#to_do_list
        is expected to be the percentage of file completed in status
        The track record's path is in self.trackRecordsPaths['download_track_record'] of the job, which
        must have been set when deploying the service program on the local server
        :return: List[str]
        '''
        # connect to the source server
        sourceServer = sftpHandle.sftpHandle()
        # locate the requested product on the source machine
        productDirOnSource = sourceServer.searchSubDir(self.productName)
        if len(productDirOnSource) == 0:
            print(f"Failed to find {self.productName} on CloudSat server. Exit.", sys.stderr)
            sourceServer.close()
            return False
        if len(productDirOnSource) > 1:
            print(
                f"More than 1 folders on CloudSat server are found to contain the requested product: {productDirOnSource}. The first one is selected.",
                file=sys.stderr)
        productDirOnSource = productDirOnSource[0]

        # now, search for a list of files under the product directory
        productFilesPaths = sourceServer.fileList(productDirOnSource)

        # then, further filter the files based on dateRange
        targetFiles = list(filter(lambda s: common_functions.commonFunctions.is_within_range(*self.dateRange, common_functions.commonFunctions.file_path_to_tai(s)), productFilesPaths))


        # build a dictionary that is the content of the track record
        downloadTrackRecordContent = {'progress': 0.0, 'to_do_list': targetFiles, 'downloaded_files': []}
        parsingTrackRecordContent = {'progress': 0.0, 'to_do_list': targetFiles, 'parsed': []}
        # write the dict to json to construct a track record
        targetFiles = []
        downloadTrackRecordFilePath = self.trackRecordsPaths['download_track_record']
        parsingTrackRecordFilePath = self.trackRecordsPaths['parsing_track_record']
        downloadTRJsonObject = json.dumps(downloadTrackRecordContent, indent=4)
        parsingTRJsonObject = json.dumps(parsingTrackRecordContent, indent=4)
        with open(downloadTrackRecordFilePath, "w") as jsonFile:
            jsonFile.write(downloadTRJsonObject)
        with open(parsingTrackRecordFilePath, 'w') as jsonFile:
            jsonFile.write(parsingTRJsonObject)

        return targetFiles

    def getABatchOfData(self):
        '''
        Download a batch of data from the source server to local, until we reach the
        spaceLimit (i.e., batch size) or all target files have once been downloaded
        and processed.

        We maintain a file that tracks the progress of the overall download task of the job
        The protocol of the record file is:
        productName
        productFolderLocationOnSourceServer
        DownloadedHdfFiles

        productName: One of {"2B-GEOPROF.P1_R05", ...}
        '''
        # connect to the source server
        sourceServer = sftpHandle.sftpHandle()

        # read the progress of the downloading job, then parse it
        progressRecord = self.trackRecordsPaths['download_track_record']
        with open(progressRecord, 'r') as recordFile:
            progressRecordContent = json.load(recordFile)
        download_progress = progressRecordContent['progress']
        msg = f'Start downloading a new batch of data. Current progress: {download_progress}.'
        self.saveJobLog(msg)
        downloaded_files = progressRecordContent['downloaded_files']
        todo_list = progressRecordContent['to_do_list']

        downloadedCt = len(downloaded_files)
        leftFilesCt = len(todo_list)
        overallTargetsCt = downloadedCt + leftFilesCt

        # continue the downloading by one batch
        dataSpacePathDataSize = 0
        dataSpacePathDataRealSize = common_functions.commonFunctions.get_folder_size(self.workingDir)
        if dataSpacePathDataSize != dataSpacePathDataRealSize:
            msg = f"Warning, the temp data dir is not cleared. The size is {dataSpacePathDataRealSize} now."
            self.saveJobLog(msg, err=True)
        while dataSpacePathDataSize < self.spaceLimit and leftFilesCt > 0:
            filePathOnSource = todo_list[0]
            dataSpacePathDataSize += sourceServer.downloadFileFromSourceToLocal(filePathOnSource, self.workingDir)
            # update the track record
            downloaded_files.append(filePathOnSource)
            todo_list.pop(0)
            leftFilesCt -= 1
            downloadedCt += 1
            download_progress = float(downloadedCt)/float(overallTargetsCt)
            msg = f"Successful download: {filePathOnSource}. {dataSpacePathDataSize / (1024 * 1024)} MB of things downloaded. \nProgress: {download_progress}."
            self.saveJobLog(msg)
            # update the track record
            newTrackRecordContent = {'progress': download_progress,
                                     'to_do_list': todo_list, 'downloaded_files': downloaded_files}
            newTRJsonObj = json.dumps(newTrackRecordContent, indent=4)
            with open(progressRecord, 'w') as recordFile:
                recordFile.write(newTRJsonObj)
            if todo_list == []:
                msg = f'Downloading is completed.'
                self.saveJobLog(msg)
                sourceServer.close()
                return True
        sourceServer.close()
        return True

    def readABatchOfData(self, fieldNames, databasePath, footprintPks = None):
        '''
        Read all data of the jobId in workingDir and store them in a database
        Maintain a track record json file at self.trackRecordsPaths['parsing_track_record']
        , whose protocol format is:
        json{'progress': int (% of hdf-eos files parsed), 'parsed': [path2hdf-eosFileOnSource],
        'to_do_list': [path2hdf-eosFileOnSource], }
        :return: List[filePathsThatAreSuccessfullyRead]
        '''
        # get the progress of the parsing tasks of the job
        trackRecordFile = self.trackRecordsPaths['parsing_track_record']
        with open(trackRecordFile, 'r') as recordFile:
            taskRecord = json.load(recordFile)
        progress = taskRecord['progress']
        todo_list = taskRecord['to_do_list']
        parsedFiles = taskRecord['parsed']
        parsedCt = len(parsedFiles)
        overallFilesCt = parsedCt + parsedCt

        msg = f'Continue the parsing task. {progress} Completed: {parsedCt}/{overallFilesCt}.'
        self.saveJobLog(msg)

        # get the list of the workingDir now
        fileBatch = common_functions.commonFunctions.list_files_under_folder(self.workingDir)
        # we should only parse the files in the todo list
        fileBatch = list(filter(lambda s: s in todo_list, fileBatch))

        msg = f'The following files will be parsed and dumped into database: {fileBatch}.'
        self.saveJobLog(msg)

        if footprintPks == None:
            # by default, the pk of a footprint (x, y, t) would use the following
            footprintPks = ["TAI_start", "Profile_time"]

        msg = f'Start parsing the batch of data with footprintPks: {footprintPks}.'
        self.saveJobLog(msg)
        self.hdfDataParser.dumpHdfSwathDataToDatabase(self.workingDir, fieldNames, footprintPks, databasePath)
        msg = f'Completed parsing the batch of data.'
        self.saveJobLog(msg)

        # delete all parsed hdf-eos files
        common_functions.commonFunctions.delete_files(fileBatch)

        msg = f'Deleted the batch of files that were just parsed: {fileBatch}.'
        self.saveJobLog(msg)

        # the batch of data is processed, update the progress
        parsedFiles = parsedFiles.extend(fileBatch)
        parsedCt += len(fileBatch)
        todo_list = list(map(lambda s: s not in fileBatch))
        progress = float(parsedCt)/float(overallFilesCt)
        parsingTRContent = {'progress': progress, 'to_do_list': todo_list,
                               'parsed': parsedFiles}
        parsingTRJsonObject = json.dumps(parsingTRContent, indent=4)
        with open(trackRecordFile, 'w') as jsonFile:
            jsonFile.write(parsingTRJsonObject)


        return fileBatch

    def filterABatchOfData(self):
        '''
        Apply SQL query on the parsed batches of data
        The program DOESN'T maintain any progress track records, but it tracks the progress
        using the most current parsing progress track record
        NOTE: Downloading task is stand-alone in parallel, but parsing and filter are in
        serial, because we must make sure only one thread is modifying the database!!
        :return: filtered_dataframe
        '''
        msg = f'Start filtering a batch of data.'
        self.saveJobLog(msg)

        filteredDF = self.dataFilter.dataFilterDF(self.filterCriteria)

        msg = f'Filtering completed for the batch.'
        self.saveJobLog(msg)
        return filteredDF


    def runDownloadThread(self, timeOutLimit = 1800):
        '''
        Thread for downloading task
        Make sure that the job has been initialized
        :return:
        '''
        progress, todoFiles, downloadedFiles = self.parseTrackRecord('download')
        start_time = time.time()
        exeTime = 0
        while todoFiles != [] and exeTime <= timeOutLimit:
            self.getABatchOfData()

            end_time = time.time()
            exeTime = end_time - start_time

    def runLocalProcessingThread(self, timeOutLimit = 9999999999):
        '''
        Thread for local processing. E.g., data parsing, dumping, and filtering
        :param timeOutLimit:
        :return:
        '''
        progress, todoFiles, parsedFiles = self.parseTrackRecord('parse')
        downProgress, tobeDownFiles, downloadedFiles = self.parseTrackRecord('download')
        start_time = time.time()
        exeTime = 0
        while todoFiles != [] and exeTime <= timeOutLimit:
            # Extract file names from downloadedFiles
            downloadedFiles = {os.path.basename(path) for path in downloadedFiles}

            # List all files in workingDir
            workingFiles = os.listdir(self.workingDir)

            # Filter out files that are in workingDir and in todoFileNames
            files = [file for file in workingFiles if file in downloadedFiles]

            if files:
                fieldNames = self.fieldNames
                if fieldNames == ['*']:
                    if self.productName == '2B-GEOPROF.P1_R05':
                        # fill in all fields of 2b-geoprof
                        fieldNames = ["Latitude", "Longitude", "UTC_start", "Height", "Data_quality", "Data_status",
                                      "Data_targetID", "RayStatus_validity", "SurfaceHeightBin",
                                      "SurfaceHeightBin_fraction", "Gaseous_Attenuation", "Sigma-Zero",
                                      "MODIS_Cloud_Fraction", "CPR_Echo_Top", "sem_NoiseFloor", "sem_NoiseFloorVar",
                                      "sem_NoiseGate", "sem_MDSignal", "Radar_Reflectivity", "MODIS_cloud_flag",
                                      'DEM_elevation', 'CPR_Cloud_mask', 'Clutter_reduction_flag', 'Vertical_binsize',
                                      'Pitch_offset', 'Roll_offset', 'Navigation_land_sea_flag', 'MODIS_Cloud_Fraction',
                                      'MODIS_scene_char', 'MODIS_scene_var']
                self.readABatchOfData(fieldNames=fieldNames, databasePath=self.databasePath)
                self.jobThumb = self.filterABatchOfData()

            # Wait before checking again
            time.sleep(10)

            end_time = time.time()
            exeTime = end_time - start_time


    def runJob(self, retry = False, downloadTimeOutLimit = 1800):
        '''
        Based on which type the job is, run the job.
        retry means whether the job once failed and is retrying.
        :return: int
        '''
        logMsg = f'Start running a {self.jobType} job: {self.jobId}.'
        self.saveJobLog(logMsg)
        if self.jobType == 'Query':
            # implement data query jobs here

            if not retry:
                # Create the directory if it does not exist
                os.makedirs(self.workingDir, exist_ok=True)
                # clear the data space
                shutil.rmtree(self.workingDir)
                os.makedirs(self.workingDir)

                # get the target list on the source server and initialize the job progress
                self.targetFileListOnSourceServer()


            # keep running the job until the job is completed
            downloadThread = threading.Thread(target=self.runDownloadThread, args=(downloadTimeOutLimit, ))
            localProcessThread = threading.Thread(target=self.runLocalProcessingThread)

            downloadThread.start()
            localProcessThread.start()

            downloadThread.join()
            localProcessThread.join()







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
        self.jobIdList = set()
        self.hdfDataParser = hdfDataParsing.hdfDataParsing()

    def jobId2SpacePaths(self, jobId):
        '''
        :param jobId: int or str
        :return: (fileTrackRecordsFilePath, spaceFolderPath)
        '''
        tractkecordsFilePath = self.tempDataDir + f"/{jobId}.txt"
        spaceFolderPath = self.tempDataDir + f"/{jobId}"

        return (tractkecordsFilePath, spaceFolderPath)



'''
tempDataDir = "/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/tempDataFolder"
CSDM = CloudSatDataManager(tempDataDir)
print(CSDM.getABatchOfData("0000", "1B-CPR.P_R05"))

jobId = '0000'
fieldNames = ["Height", "Data_quality", "Radar_Reflectivity", "MODIS_cloud_flag", 'DEM_elevation', 'CPR_Cloud_mask', 'Clutter_reduction_flag', 'Vertical_binsize', 'Pitch_offset', 'Roll_offset', 'Navigation_land_sea_flag', 'MODIS_Cloud_Fraction', 'MODIS_scene_char', 'MODIS_scene_var']
databasePath = './testDB'
tempDataDir = "/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/tempDataFolder"
CSDM = CloudSatDataManager(tempDataDir)
CSDM.readABatchOfData(jobId, fieldNames, databasePath)

jobId = '0000'
fieldNames = ["Height", "Data_quality", "Radar_Reflectivity", "MODIS_cloud_flag", 'DEM_elevation', 'CPR_Cloud_mask', 'Clutter_reduction_flag', 'Vertical_binsize', 'Pitch_offset', 'Roll_offset', 'Navigation_land_sea_flag', 'MODIS_Cloud_Fraction', 'MODIS_scene_char', 'MODIS_scene_var']
databasePath = './testDB'
tempDataDir = "/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/tempDataFolder"
CSDM = CloudSatDataManager(tempDataDir)
CSDM.readABatchOfData(jobId, fieldNames, databasePath)
'''

WD = '/Users/leo.li27/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/dataThroughPutRecords/0001'
trackRecordFilePaths = {'download_track_record': './0001Download.json',
                        'parsing_track_record': './0001Parse.json'}
filterCriteria = {'Height': ('3d', '>=', 1200)}
testJob = CloudSatJobs('0001', WD, 100000000, trackRecordFilePaths,
                       ['2012-02-03T00:00:00', '2012-02-04T00:00:00'], filterCriteria=filterCriteria)
testJob.runJob(retry=True)