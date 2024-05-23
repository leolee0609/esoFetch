import json


def param2json(jsonPath, **param):
    paramJsonObj = json.dumps(param)
    with open(jsonPath, 'w') as jsonFile:
        jsonFile.writelines(paramJsonObj)



filterCriteria = {'Height': ('3d', '>=', 1200)}
param2json(jsonPath='./tempRequests/test.json',
           jobId = '0002',
           jobType = 'Query',
            dateRange = ['2012-02-03T00:00:00', '2012-02-04T00:00:00'],
           filterCriteria=filterCriteria
           )

'''
WD = '../dataThroughPutRecords/0001'
trackRecordFilePaths = {'download_track_record': './0001Download.json',
                        'parsing_track_record': './0001Parse.json'}
param2json(jsonPath='./tempRequests/0001_request.json',
           jobId='0001', workingDir=WD, spaceLimit=20000000000, trackRecordsPaths=trackRecordFilePaths,
           dateRange=['2012-02-03T00:00:00', '2012-02-04T00:00:00'], filterCriteria=filterCriteria
           )
'''
