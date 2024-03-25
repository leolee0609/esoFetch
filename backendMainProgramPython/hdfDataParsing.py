# this module encapsulates functionalities for parsing hdf-eos data
import matlab.engine

class hdfDataParsing:
    def __init__(self, matlabScriptsFolder = "../backendScriptsInMATLAB"):
        self.eng = matlab.engine.start_matlab()
        self.eng.cd("../backendScriptsInMATLAB")


    def readABatchOfHdfSwathData(self, folderPath, fieldNames, footprintPks):
        self.eng.readABatchOfHdfSwathData(folderPath, fieldNames, footprintPks)
