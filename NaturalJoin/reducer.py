from concurrent import futures
import json
import multiprocessing
import os
from urllib.parse import urlparse

import grpc
import Project_pb2
import Project_pb2_grpc

'''
# Assumptions:
1) Avoided sorting by using dictionary.
'''

class ReducerServicer(Project_pb2_grpc.ReducerServicer):
    def reduce(self, reducerId, intermediateLocations, outputLocation):
        print(f"Reducer-{reducerId} is running")
        
        # step 5a: shuffle
        shuffleKeyValuePairs = {}
        for location in intermediateLocations:
            partition_location = f"{location}/partition{reducerId-1}.txt"
            readFileHandler = open(partition_location, "r")
            
            line = readFileHandler.readline()
            while line:
                readRow = line[:-1].split(", ", 1)
                key = readRow[0]
                value = eval(readRow[1])
                shuffleKeyValuePairs[key] = shuffleKeyValuePairs.setdefault(key, []) + value        
                line = readFileHandler.readline()
         
        # # step 5b: sort 
        # sortedKeys = list(shuffleKeyValuePairs.keys())
        # sortedKeys.sort()
        # sortedKeyValuePairs = {key: shuffleKeyValuePairs[key] for key in sortedKeys}
        
        aggregatedKeyValuePairs = shuffleKeyValuePairs
        # step 6a: reduce and save the final key value pairs in the output files
        os.makedirs(f"{outputLocation}", exist_ok = True)
        writeFileHandler = open(f"{outputLocation}/output{reducerId}.txt", "w") 
        writeRow = f"Name, Age, Role\n"
        writeFileHandler.write(writeRow)
        for key in aggregatedKeyValuePairs:
            value = aggregatedKeyValuePairs[key]
            t1Values = []
            t2Values = []
            for tableId, value in aggregatedKeyValuePairs[key]:
                if tableId == "T1":
                    t1Values.append(value)
                else:
                    t2Values.append(value)
            
            for t1Value in t1Values:
                for t2Value in t2Values:
                    writeRow = f"{key}, {t1Value}, {t2Value}\n"
                    writeFileHandler.write(writeRow)
            
    def reducer(self, request, context):
        masterAddress = urlparse(context.peer()).path
        idx = masterAddress.rfind(':')
        # ip = masterAddress[:idx]
        ip = "localhost"
        port = masterAddress[idx+ 1:]
        print(f"REDUCE REQUEST FROM {ip}:{port}")
        
        # master request
        intermediateLocations = list(request.intermediateLocations)
        outputLocation = request.outputLocation
        reducersCount = request.reducersCount

        # step 2: fork reducer workers
        reducerWorkers = []
        for i in range(reducersCount):
            reducerId = i + 1
            reducer = multiprocessing.Process(target=self.reduce, args=(reducerId, intermediateLocations, outputLocation))
            reducerWorkers.append(reducer)

        # run reduce tasks and waits for them to finish
        for reducerWorker in reducerWorkers:
            reducerWorker.start()
        for reducerWorker in reducerWorkers:
            reducerWorker.join()
            
        return Project_pb2.ReducerResponse(status=True)
    
    
def serve(ip, port, max_workers):
    '''
    parameters:
    ip, port: ip, port on which the reducer server will aceept the requests
    max_workers: size of the thread pool to be used by the server to execute RPC handlers
    
    starts the reducer server on the specified port,
    waits for the master to send the requests
    '''    
    # creates a server with Reducer service on the specified port
    reducerServer = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    Project_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(), reducerServer)
    reducerServer.add_insecure_port(f"{ip}:{port}")
    
    # starts the server
    reducerServer.start()
    print(f"Reducer Server running on {ip}:{port}")
    reducerServer.wait_for_termination()
    
    
if __name__ == "__main__":
    # starts the reducer server
    serve(ip="localhost", port="8000", max_workers = 10)