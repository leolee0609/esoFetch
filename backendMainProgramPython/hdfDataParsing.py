# this module encapsulates functionalities for parsing hdf-eos data
import matlab.engine

class hdfDataParsing:
    def __init__(self, matlabScriptsFolder = "../backendScriptsInMATLAB"):
        self.eng = matlab.engine.start_matlab()
        self.eng.cd(matlabScriptsFolder)


    def readABatchOfHdfSwathData(self, folderPath, fieldNames, footprintPks):
        self.eng.readABatchOfHdfSwathData(folderPath, fieldNames, footprintPks)

    def dumpHdfSwathDataToDatabase(self, folderPath, fieldNames, footprintPks, databasePath):
        '''
        Parse all hdf files under folderPath to 2d-attribute and 3d-attribute datasets
        with footprintPks as the primary key (usually combined), then save them into
        database
        :param folderPath: str
        :param fieldNames: List[str]
        :param footprintPks: List[str]
        :param databasePath: str
        :return:
        '''
        self.eng.dumpHdfSwathDataToDatabase(folderPath, fieldNames, footprintPks, databasePath)

