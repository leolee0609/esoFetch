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
import datetime
import matplotlib.pyplot as plt
import multiprocessing


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
    def __init__(self, **initInfo):
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
        # unparse the initialization parameters
        jobId = initInfo.get('jobId', None)
        workingDir = initInfo.get('workingDir', None)
        spaceLimit = initInfo.get('spaceLimit', None)
        trackRecordsPaths = initInfo.get('trackRecordsPaths', None)
        dateRange = initInfo.get('dateRange', None)
        productName = initInfo.get('productName', None)
        jobType = initInfo.get('jobType', None)
        fieldNames = initInfo.get('fieldNames', None)
        filterCriteria = initInfo.get('filterCriteria', None)
        logFile = initInfo.get('logFile', None)

        # handle the default parameters
        if productName == None:
            productName = '2B-GEOPROF.P1_R05'
        if jobType == None:
            jobType = 'Query'
        if fieldNames == None:
            fieldNames = ['*']

        self.jobId = jobId
        self.productName = productName
        self.dateRange = dateRange
        self.workingDir = workingDir
        self.spaceLimit = spaceLimit
        self.jobType = jobType
        self.fieldNames = fieldNames
        if logFile == None:
            logFile = f'./jobsInfo/{jobId}_logs.txt'
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


    def plotJobThumbnail(self, plottedColumn = "Radar_Reflectivity"):
        '''
        Plot the stored thumbnail of the job and store into workDir
        You may define different ways of plotting jobThumb
        Automatically detect if the thumbnail is 2d or 3d
        If 3d, use 'TAI_start' + 'Profile_time' as x axies, 'Height' as y axies,
        plottedColumn as plotted value
        If 2d, use 'Longitude' as x axies, 'Latitude' as y axies, plottedColumn
        as plotted value

        update: The plot was so inconsistent. Should multiply the time with velocity in meter
        Now, set it to 700 000

        :return: plt, ax
        '''
        df = self.jobThumb
        if type(df) == type(None):
            msg = f"Failed to generate thumbnail for {self.jobType} job: {self.jobId}. The job doesn't have a thumbnail so far."
            self.saveJobLog(msg, err=True)
            return None, None
        if plottedColumn not in df.columns:
            msg = f"Failed to generate thumbnail for {self.jobType} job: {self.jobId}. No column called {plottedColumn}."
            self.saveJobLog(msg, err = True)
            return None, None
        if 'Height' in df.columns and 'TAI_start' in df.columns and 'Profile_time' in df.columns:
            # a 3d plot
            # the 700000 rescales the unit from a time unit to meter
            df['X_values'] = 700000 * (df['TAI_start'] + df['Profile_time'])

            # Set up the plot
            fig, ax = plt.subplots()

            # Create scatter plot
            scatter = ax.scatter(df['X_values'], df['Height'], c=df[plottedColumn], cmap='viridis')

            # Add a colorbar
            cbar = plt.colorbar(scatter)
            cbar.set_label(plottedColumn)

            # Label the axes
            ax.set_xlabel('Time')
            ax.set_ylabel('Height')

            return fig, ax

        elif 'Latitude' in df.columns and 'Longitude' in df.columns:
            # a 2d plot
            # Set up the plot
            fig, ax = plt.subplots()

            # Create scatter plot
            scatter = ax.scatter(df['Longitude'], df['Latitude'], c=df[plottedColumn], cmap='viridis')

            # Add a colorbar
            cbar = plt.colorbar(scatter)
            cbar.set_label(plottedColumn)

            # Label the axes
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

            return fig, ax

        else:
            msg = f"Failed to generate thumbnail for {self.jobType} job: {self.jobId}. Can't find positional columns."
            self.saveJobLog(msg, err=True)
            return None, None


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
        prtmsg = f'{self.jobType} job ID: {self.jobId}, job with parallel threads:'
        downloadProgress, unDownloadedFiles, downloadedFiles = self.parseTrackRecord('download')
        unDownloadedFilesCt = len(unDownloadedFiles)
        downloadedFilesCt = len(downloadedFiles)
        overallFiles = unDownloadedFilesCt + downloadedFilesCt
        parsedProgress, unparsedFiles, parsedFiles = self.parseTrackRecord('parse')
        prtmsg = (prtmsg + f'\nDownload task progress: {downloadProgress}, {unDownloadedFilesCt}/{overallFiles};\n'
                           + f'Parsing task progress: {parsedProgress}, {len(unparsedFiles)}/{overallFiles}.')
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
            taiTimeStamp = time.time()
            timeStamp = datetime.datetime.fromtimestamp(taiTimeStamp)
            info = str(timeStamp) + ': ' + info
        if err:
            printedErrMsg = f'From a {self.jobType} job {self.jobId}:\n'
            print(printedErrMsg + info, file=sys.stderr)
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

        sourceServer.close()
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


        if todo_list == []:
            # no new downloading tasks. Do nothing
            return None

        # connect to the source server
        sourceServer = sftpHandle.sftpHandle()

        # continue the downloading by one batch
        dataSpacePathDataSize = 0
        dataSpacePathDataRealSize = common_functions.commonFunctions.get_folder_size(self.workingDir)
        if dataSpacePathDataSize != dataSpacePathDataRealSize:
            msg = f"Warning, the temp data dir is not cleared. The size is {dataSpacePathDataRealSize} now."
            self.saveJobLog(msg)
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

    def readABatchOfData(self, fileBatch, fieldNames, databasePath, footprintPks = None):
        '''
        Read all data of the jobId in workingDir and store them in a database
        Maintain a track record json file at self.trackRecordsPaths['parsing_track_record']
        , whose protocol format is:
        json{'progress': int (% of hdf-eos files parsed), 'parsed': [path2hdf-eosFileOnSource],
        'to_do_list': [path2hdf-eosFileOnSource], }
        '''
        # get the progress of the parsing tasks of the job
        trackRecordFile = self.trackRecordsPaths['parsing_track_record']
        with open(trackRecordFile, 'r') as recordFile:
            taskRecord = json.load(recordFile)
        progress = taskRecord['progress']
        todo_list = taskRecord['to_do_list']
        parsedFiles = taskRecord['parsed']
        parsedCt = len(parsedFiles)
        overallFilesCt = parsedCt + len(todo_list)

        if todo_list == []:
            # nothing to be parsed. Do nothing
            return None

        msg = f'Continue the parsing task. {progress} Completed: {parsedCt}/{overallFilesCt}.'
        self.saveJobLog(msg)

        msg = f'The following files will be parsed and dumped into database: {fileBatch}.'
        self.saveJobLog(msg)

        if footprintPks == None:
            # by default, the pk of a footprint (x, y, t) would use the following
            footprintPks = ["TAI_start", "Profile_time"]

        msg = f'Start parsing the batch of data with footprintPks: {footprintPks}.'
        self.saveJobLog(msg)
        if fileBatch:
            self.hdfDataParser.dumpHdfSwathDataToDatabase(fileBatch, fieldNames, footprintPks, databasePath)
            msg = f'Completed parsing the batch of data.'
            self.saveJobLog(msg)

            # delete all parsed hdf-eos files
            common_functions.commonFunctions.delete_files(fileBatch)

            msg = f'Deleted the batch of files that were just parsed: {fileBatch}.'
            self.saveJobLog(msg)

            # the batch of data is processed, update the progress
            # convert local file paths in fileBatch to remote paths
            currentParsedFiles = list(map(lambda s: s.split('/')[-1], fileBatch))
            for todoFile in todo_list:
                for currentParsedFile in currentParsedFiles:
                    if currentParsedFile in todoFile:
                        parsedFiles.append(todoFile)
            parsedCt = len(parsedFiles)
            todo_list = list(filter(lambda s: s not in parsedFiles, todo_list))
            progress = float(parsedCt)/float(overallFilesCt)
            parsingTRContent = {'progress': progress, 'to_do_list': todo_list,
                                   'parsed': parsedFiles}
            parsingTRJsonObject = json.dumps(parsingTRContent, indent=4)
            with open(trackRecordFile, 'w') as jsonFile:
                jsonFile.write(parsingTRJsonObject)

            return True
        else:
            return False

    def filterABatchOfData(self):
        '''
        Apply SQL query on the parsed batches of data
        The program DOESN'T maintain any progress track records, but it tracks the progress
        using the most current parsing progress track record
        NOTE: Downloading task is stand-alone in parallel, but parsing and filter are in
        serial, because we must make sure only one thread is modifying the database!!
        :return: filtered_dataframe
        '''
        import sqlite3
        conn = sqlite3.connect(self.databasePath)

        # Create a cursor object
        cursor = conn.cursor()

        # Execute the SQL query to show tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

        # Fetch all the tables
        tables = cursor.fetchall()

        # Print the tables
        for table in tables:
            print(table[0])

        # Close the cursor and connection
        cursor.close()
        conn.close()

        msg = f'Start filtering a batch of data.'
        self.saveJobLog(msg)

        try:
            filteredDF = self.dataFilter.dataFilterDF(self.filterCriteria)
        except Exception as e:
            print(f"Warning: Failed to filter data with error: {e}")


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
            if common_functions.commonFunctions.get_folder_size(self.workingDir) <= self.spaceLimit:
                condition = self.getABatchOfData()

                if condition == None:
                    # downloading task is completed
                    msg = f"Downloading task of the {self.jobType} job: {self.jobId} is completed."
                    print(msg)
                    self.saveJobLog(msg)
                    return

                end_time = time.time()
                exeTime = end_time - start_time

    def runLocalProcessingThread(self, timeOutLimit = 99999999999999999999):
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
            # get all files in the todo list of parsing, and downloaded in the downloading
            progress, todoFiles, parsedFiles = self.parseTrackRecord('parse')
            downProgress, tobeDownFiles, downloadedFiles = self.parseTrackRecord('download')
            downloadedFiles = list(map(lambda s: s.split('/')[-1], downloadedFiles))
            files = list(map(lambda s: s.split('/')[-1], todoFiles))
            # find the todoFiles that are already downloaded. Those are to be parsed
            files = list(filter(lambda s: s in downloadedFiles, files))
            files = list(map(lambda s: self.workingDir + '/' + s, files))
            # don't dump too many one time. Memory will exceed
            files = files[:15]


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

                condition = self.readABatchOfData(fileBatch=files, fieldNames=fieldNames, databasePath=self.databasePath)
                self.jobThumb = self.filterABatchOfData()

                if condition == None:
                    # parsing task is completed
                    msg = f"Parsing task of the {self.jobType} job: {self.jobId} is completed."
                    print(msg)
                    self.saveJobLog(msg)
                    return

            # Wait before checking again
            time.sleep(10)

            end_time = time.time()
            exeTime = end_time - start_time


    def runJob(self, retry = False, downloadTimeOutLimit = 1800):
        '''
        Based on which type the job is, run the job.
        retry means whether the job once failed and is retrying.
        :return: bool: succeeded or failed
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

            # show and save the thumbnail of the job
            fig, ax = self.plotJobThumbnail()
            # Save the figure
            fig.savefig(f"{self.workingDir}/{self.jobId}_thumbnail.png")  # Saves the plot as a PNG file

            msg = f'Job: {self.jobId} is completed.'
            self.saveJobLog(msg)
            print(msg)
            return True

        return False



class CloudSatDataManager:
    '''
    A CloudSatDataManager object is responsible for one local server.
    It initializes and manages CloudSatJobs, and interacts with outside
    callers, e.g., frontend frameworks.

    dataThroughputDir: Path to the directory that you want to use for throughput data
    serviceSpaceSize: Maximum disk use for the service. Unit: Bytes
    1GB = 1024 * 1024 * 1024 by default
    '''
    def __init__(self, dataThroughputDir = '/mnt/data/obs/CloudSatDataQueryJobs', serviceSapceSize = 30000000000000, logFile = './serviceLogs.txt'):
        self.dataThroughputDir = dataThroughputDir
        self.serviceSpaceSize = serviceSapceSize
        self.jobIdList = set()  # all the jobs' id received. In oneof{completed, not_ran, failed, running}
        self.jobList = []  # all the job objects
        self.runningJobList = {}  # Dict{jobId: multiprocessingProcess}
        self.jobStatusRecord = './jobStatus.json'
        jobStatusRecord = {
        'completed_jobs': [],
        'failed_jobs': [],
        'not_ran_jobs': [],  # the jobs that have not yet ran. Should be initialized by initAJob()
        'running_jobs': []
        }
        jobStatusRecordJsonObj = json.dumps(jobStatusRecord)
        with open(self.jobStatusRecord, 'w') as jobStatusFile:
            jobStatusFile.write(jobStatusRecordJsonObj)

        self.hdfDataParser = hdfDataParsing.hdfDataParsing()
        self.logFile = logFile
        # if the log files doesn't exist, create one
        if not os.path.exists(self.logFile):
            with open(self.logFile, 'w') as f:
                f.write("")

        self.maxJobsRunning = 10


    def saveJobLog(self, info, err = False, timeStamp = True):
        '''
        Save info to
        :param str
        :return: str
        '''
        if info[-1:] != '\n':
            # if there is no line breakers, append one to the end of the log message
            info = info + '\n'
        if timeStamp:
            # add a time stamp
            taiTimeStamp = time.time()
            timeStamp = datetime.datetime.fromtimestamp(taiTimeStamp)
            info = str(timeStamp) + ': ' + info
        if err:
            print(info, file=sys.stderr)
        with open(self.logFile, 'a') as file:
            print(info, file=file)
        return info

    def jobId2SpacePaths(self, jobId):
        '''
        The function distributes a working space for the job with jobId
        Return info includes Dict{jobProgressTrackRecordName: path2it} and the direcotry under which
        the job is run
        :param jobId: int or str
        :return: (fileTrackRecordsFilePath, spaceFolderPath)
        '''
        trackRecordFilePaths = {}
        trackRecordFilePaths['download_track_record'] = f'./jobsInfo/{jobId}_downloading_task.json'
        trackRecordFilePaths['parsing_track_record'] = f'./jobsInfo/{jobId}_parsing_task.json'

        downloadRecord = {
            'progress': 0.0,
            'to_do_list': [],
            'downloaded_files': []
        }

        parsingRecord = {
            'progress': 0.0,
            'to_do_list': [],
            'parsed': []
        }

        downloadRecordJsonObj = json.dumps(downloadRecord)
        parsingRecordJsonObj = json.dumps(parsingRecord)

        with open(trackRecordFilePaths['download_track_record'], 'w') as trackRecordFile:
            trackRecordFile.write(downloadRecordJsonObj)

        with open(trackRecordFilePaths['parsing_track_record'], 'w') as trackRecordFile:
            trackRecordFile.write(parsingRecordJsonObj)

        spaceFolderPath = self.dataThroughputDir + f"/{jobId}"

        return (trackRecordFilePaths, spaceFolderPath)

    def jobId2SpaceSize(self):
        '''
        Implement space size distribution algorithms here
        :param jobId: int or str
        :return: int. Unit: Bytes
        '''
        sizeUsed = common_functions.commonFunctions.get_folder_size(self.dataThroughputDir)
        minSpace4Job = 10 * 1024 * 1024 * 1024
        return minSpace4Job




        if sizeUsed > self.serviceSpaceSize:
            return 0
        sizeLeft = self.serviceSpaceSize - sizeUsed
        if sizeLeft < minSpace4Job:
            return 0
        else:
            return min([minSpace4Job, sizeLeft / 3])

    def initAJob(self, **jobInfo):
        '''
        The function initializes a CloudSatJob and append it to the job list
        :param jobInfo: Dict{jobAttribute: valueForJobInitialization}
        :return: jobId or None if failed
        '''
        if jobInfo.get('jobType') == 'Query':
            # this is a query job, do the implementation here:
            initInfo = {}
            initInfo.update(jobInfo)

            # The received info shouldn't include the info about how we manage it
            # so we should handle it here
            jobId = jobInfo.get('jobId')
            # distribute a work space for the job
            jobTrackRecordsFilePaths, jobSpace = self.jobId2SpacePaths(jobId)
            initInfo['workingDir'] = jobSpace
            initInfo['trackRecordsPaths'] = jobTrackRecordsFilePaths
            # distribute a space for the job
            jobSpaceSize = self.jobId2SpaceSize()
            initInfo['spaceLimit'] = jobSpaceSize

            # now, pass the parameters to initialize a CloudSat data query job
            try:
                job = CloudSatJobs(**initInfo)
                # save log
                msg = f'A new job {jobId} is created successfully: {initInfo}.'
                self.saveJobLog(msg)

                return (jobId, job)
            except Exception as e:
                msg = str(e)
                self.saveJobLog(msg, err=True)


        else:
            msg = f'The job is not supported so far. Request rejected.'
            self.saveJobLog(msg, err=True)

    def receiveRequest(self):
        '''
        Listen to requests, then parse and initialize it as a job.
        :return:
        '''
        already_seen_files = set(os.listdir('./requests'))  # Track files already parsed

        while True:
            # receive requests every 1 second
            time.sleep(1)  # Check every 1 second
            current_files = set(os.listdir('./requests'))
            new_files = current_files - already_seen_files

            for file_name in new_files:
                if file_name.endswith('.json'):
                    full_path = os.path.join('./requests', file_name)
                    processedPath = './requests/processed_requests'
                    jobInitInfo = common_functions.commonFunctions.parse_json(full_path, processedPath)
                    msg = f'Received a request: {jobInitInfo}.'
                    self.saveJobLog(msg)
                    print(msg)

                    # handle the request
                    jobId, job = self.initAJob(**jobInitInfo)
                    # update service status
                    if jobId:
                        self.jobIdList.add(jobId)
                        self.jobList.append(job)

                        with open(self.jobStatusRecord, 'r') as statusRecord:
                            statusDict = json.load(statusRecord)

                        statusDict['not_ran_jobs'].append(jobId)
                        statusJsonObj = json.dumps(statusDict, indent=4)
                        with open(self.jobStatusRecord, 'w') as statusRecord:
                            statusRecord.write(statusJsonObj)
                    elif jobId == None:
                        msg = f'Failed to create job: {jobInitInfo}.'
                        self.saveJobLog(msg, err = True)
                        continue

                    msg = f'job: {jobId} created.'
                    self.saveJobLog(msg)
                    print(self.jobList[-1])

            already_seen_files.update(new_files)


    def manageJobs(self):
        '''
        Manage the jobs so that the service is run within allowed space size.
        Monitor and retry failed jobs.
        This function now runs jobs in parallel using processes.
        Maintain jobStatus.json here:
        {
        completed_jobs: [],
        failed_jobs: [],
        not_ran_jobs: [],  # the jobs that have not yet ran. Should be initialized by initAJob()
        running_jobs: []
        }
        :return:
        '''
        # initialize and start/retry the not_ran/failed jobs
        while True:
            # parse the service status
            with open(self.jobStatusRecord, 'r') as statusRecord:
                jobStatus = json.load(statusRecord)

            # first, monitor the running job
            for jobId, runningJobProcess in self.runningJobList.items():
                if runningJobProcess == None:
                    # the job has been completed. Ignore it.
                    continue

                if not runningJobProcess.is_alive():
                    # the job terminates
                    msg = f'Job {jobId} terminates.'
                    self.saveJobLog(msg)
                    print(jobId)
                    print(jobStatus['running_jobs'])
                    jobStatus['running_jobs'].remove(jobId)
                    self.runningJobList[jobId] = None

                    # check if the job is successful:
                    for job in self.jobList:
                        if job.jobId == jobId:
                            jobProgressRecordPaths = job.trackRecordsPaths
                            jobParsingTaskRecordPath = jobProgressRecordPaths['parsing_track_record']
                            with open(jobParsingTaskRecordPath, 'r') as trackRecordsFile:
                                jobProgressDict = json.load(trackRecordsFile)
                            progress = jobProgressDict['progress']
                            if progress < 1:
                                msg = f'Job {jobId} failed. Progress: {progress}.'
                                self.saveJobLog(msg, err = True)
                                jobStatus['failed_jobs'].append(jobId)
                            else:
                                # the job is completd
                                jobStatus['completed_jobs'].append(jobId)

            # then, manage the idling jobs
            for job in self.jobList:
                if job.jobId in jobStatus['not_ran_jobs']:
                    # the job is not ran
                    # start jobs until the max allowed disk size is used up
                    sizeUsed = common_functions.commonFunctions.get_folder_size(self.dataThroughputDir)
                    sizeLeft = self.serviceSpaceSize - sizeUsed
                    if sizeLeft > job.spaceLimit:
                        # have room for this job
                        # run it
                        msg = f'Service size is sufficient for job: {job.jobId} ({sizeLeft}/{self.serviceSpaceSize}). Start running the job.'
                        self.saveJobLog(msg)

                        # jobProcess = multiprocessing.Process(target=job.runJob, args=(False, ))
                        # jobProcess.start()
                        jobProcess = threading.Thread(target=job.runJob, args=(False, ))
                        jobProcess.start()

                        self.runningJobList[job.jobId] = jobProcess

                        # update the service status
                        jobStatus['not_ran_jobs'].remove(job.jobId)
                        jobStatus['running_jobs'].append(job.jobId)


                elif job.jobId in jobStatus['failed_jobs']:
                    # the job was failed
                    # start jobs until the max allowed disk size is used up
                    sizeUsed = common_functions.commonFunctions.get_folder_size(self.dataThroughputDir)
                    sizeLeft = self.serviceSpaceSize - sizeUsed
                    if sizeLeft > job.spaceLimit:
                        # have room for this job
                        # run it
                        msg = f'Service size is sufficient for job: {job.jobId} ({sizeLeft}/{self.serviceSpaceSize}). Restart the failed job.'
                        self.saveJobLog(msg)

                        # jobProcess = multiprocessing.Process(target=job.runJob, args=(True,))
                        # jobProcess.start()
                        jobProcess = threading.Thread(target=job.runJob, args=(True, ))
                        jobProcess.start()

                        self.runningJobList[job.jobId] = jobProcess

                        # update the service status
                        jobStatus['failed_jobs'].remove(job.jobId)
                        jobStatus['running_jobs'].append(job.jobId)

            # update the job status record
            jobStatusJsonObj = json.dumps(jobStatus, indent=4)
            with open(self.jobStatusRecord, 'w') as statusRecord:
                statusRecord.write(jobStatusJsonObj)


            time.sleep(30)


    def runService(self):
        '''
        Process requests, initialize, Start, monitor, re-run existed jobs
        Threading:
            receiveRequest
            ManageJobs
        :return:
        '''
        msg = f'Service starts.'
        self.saveJobLog(msg)

        # start the threads
        receiveRequestThread = threading.Thread(target=self.receiveRequest)
        jobManagementThread = threading.Thread(target=self.manageJobs)

        receiveRequestThread.start()
        jobManagementThread.start()

        receiveRequestThread.join()
        jobManagementThread.join()






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



WD = '../dataThroughPutRecords/0001'
trackRecordFilePaths = {'download_track_record': './0001Download.json',
                        'parsing_track_record': './0001Parse.json'}
filterCriteria = {'Height': ('3d', '>=', 1200)}
testJob = CloudSatJobs('0001', WD, 20000000000, trackRecordFilePaths,
                       ['2012-02-03T00:00:00', '2012-02-04T00:00:00'], filterCriteria=filterCriteria)
testJob.runJob(retry=False)



WD = '../dataThroughPutRecords/0001'
trackRecordFilePaths = {'download_track_record': './0001Download.json',
                        'parsing_track_record': './0001Parse.json'}
filterCriteria = {'Height': ('3d', '>=', 1200)}
testJob = CloudSatJobs(jobId='0001', workingDir=WD, spaceLimit=20000000000, trackRecordsPaths=trackRecordFilePaths,
                       dateRange = ['2012-02-03T00:00:00', '2012-02-04T00:00:00'], filterCriteria=filterCriteria)
testJob.runJob(retry=False)


with open('./tempRequests/0001_request.json', 'r') as file:
    para = json.load(file)
testJob = CloudSatJobs(**para)
testJob.runJob(retry=False)
'''

CloudSatDataManager().runService()
