# this module encapsulates functionalities for parsing hdf-eos data
import sys

import matlab.engine

class hdfDataParsing:
    def __init__(self, matlabScriptsFolder = "../backendScriptsInMATLAB"):
        self.eng = matlab.engine.start_matlab()
        self.eng.cd(matlabScriptsFolder)


    def readABatchOfHdfSwathData(self, filePaths, fieldNames, footprintPks):
        self.eng.readABatchOfHdfSwathData(filePaths, fieldNames, footprintPks)

    def dumpHdfSwathDataToDatabase(self, filePaths, fieldNames, footprintPks, databasePath):
        '''
        Parse all hdf files under folderPath to 2d-attribute and 3d-attribute datasets
        with footprintPks as the primary key (usually combined), then save them into
        database
        :param filePaths: List[str]
        :param fieldNames: List[str]
        :param footprintPks: List[str]
        :param databasePath: str
        :return:
        '''
        try:
            self.eng.dumpHdfSwathDataToDatabase(filePaths, fieldNames, footprintPks, databasePath)
        except Exception as e:
            # sometimes a table might be inserted multiple times, which will lead to primary key
            #   uniqueness constraints violation. Handle that to avoid program collapsing.
            print(e, file=sys.stderr)
'''
filePaths = ["/Users/liding/Documents/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/dataThroughPutRecords/0001/2012035070738_30700_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf", "/Users/liding/Documents/Documents/uwaterloo/research/CloudSat/CloudSatWebProjects/dataThroughPutRecords/0001/2012035233520_30710_CS_2B-GEOPROF_GRANULE_P1_R05_E05_F00.hdf"]
fieldNames = ["Latitude", "Longitude", "UTC_start", "Height", "Data_quality", "Data_status", "Data_targetID", "RayStatus_validity", "SurfaceHeightBin", "SurfaceHeightBin_fraction", "Gaseous_Attenuation", "Sigma-Zero", "MODIS_Cloud_Fraction", "CPR_Echo_Top", "sem_NoiseFloor", "sem_NoiseFloorVar", "sem_NoiseGate", "sem_MDSignal", "Radar_Reflectivity", "MODIS_cloud_flag", 'DEM_elevation', 'CPR_Cloud_mask', 'Clutter_reduction_flag', 'Vertical_binsize', 'Pitch_offset', 'Roll_offset', 'Navigation_land_sea_flag', 'MODIS_Cloud_Fraction', 'MODIS_scene_char', 'MODIS_scene_var'];
footprintPks = ["TAI_start", "Profile_time"]
dbPath = "../db.sqlite3"
hdfDataParsing().dumpHdfSwathDataToDatabase(filePaths, fieldNames, footprintPks, dbPath)
'''



