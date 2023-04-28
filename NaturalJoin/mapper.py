from concurrent import futures
import multiprocessing
import os
from urllib.parse import urlparse

import grpc
import Project_pb2
import Project_pb2_grpc

class MapperServicer(Project_pb2_grpc.MapperServicer):
    def map(self, mapperId, shard, reducersCount, intermediateLocations):
        print(f"Mapper-{mapperId} is running")

        intermediateKeyValuePairs = {}
        # reading each file one by one
        for file in shard.files:
            # generating intermediate key-value pairs
            fileId = file.id
            readFileHandler = open(file.location, "r")
            
            line = readFileHandler.readline()
            columns = line[:-1].split(", ")
            keyColumn, ValueColumn = [0,1] if columns[0] == "Name" else [1,0]
            
            line = readFileHandler.readline()
            while line:
                readRow = line.strip().split(", ", maxsplit=1)
                # intermediateKeyValuePairs.setdefault((columns[0] + ":" + readRow[0]), []).append((fileId, (columns[1] + ":" + readRow[1])))
                key = readRow[keyColumn]
                tableId = "T1" if "Age" in columns else "T2"
                value = (tableId, readRow[ValueColumn])
                intermediateKeyValuePairs.setdefault(key, []).append(value)
                line = readFileHandler.readline()
            
        # step 4: creating intermediate files & partitioning the data
        os.makedirs(f"mapper{mapperId}", exist_ok = True)
        writeFileHandlers = []
        for partitionId in range(reducersCount):
            writeFileHandler = open(f"mapper{mapperId}/partition{partitionId}.txt", "w")
            writeFileHandlers.append(writeFileHandler)
            
        for key, value in intermediateKeyValuePairs.items():
            writeRow = str(key) + ", " + str(value) + "\n"
            partitionId = len(str(key)) % reducersCount
            writeFileHandlers[partitionId].write(writeRow)
            
        intermediateLocations.append(f"mapper{mapperId}")
        
    def mapper(self, request, context):
        masterAddress = urlparse(context.peer()).path
        idx = masterAddress.rfind(':')
        # ip = masterAddress[:idx]
        ip = "localhost"
        port = masterAddress[idx+ 1:]
        print(f"MAP REQUEST FROM {ip}:{port}")
        
        # master request
        shards = request.shards
        mappersCount = request.mappersCount
        reducersCount = request.reducersCount
        
        # step 2: fork mapper workers
        manager = multiprocessing.Manager()
        intermediateLocations = manager.list()
        mapperWorkers = []
        for i in range(mappersCount):
            mapperId = i + 1
            mapper = multiprocessing.Process(target=self.map, args=(mapperId, shards[i], reducersCount, intermediateLocations))
            mapperWorkers.append(mapper)

        # step 3: run map tasks and waits for them to finish
        for mapperWorker in mapperWorkers:
            mapperWorker.start()
        for mapperWorker in mapperWorkers:
            mapperWorker.join()

        return Project_pb2.MapperResponse(status=True, intermediateLocations = intermediateLocations)
    
    
def serve(ip, port, max_workers):
    '''
    parameters:
    ip, port: ip, port on which the mapper server will aceept the requests
    max_workers: size of the thread pool to be used by the server to execute RPC handlers
    
    starts the mapper server on the specified port,
    waits for the master to send the requests
    '''    
    # creates a server with Mapper service on the specified port
    mapperServer = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    Project_pb2_grpc.add_MapperServicer_to_server(MapperServicer(), mapperServer)
    mapperServer.add_insecure_port(f"{ip}:{port}")
    
    # starts the server
    mapperServer.start()
    print(f"Mapper Server running on {ip}:{port}")
    mapperServer.wait_for_termination()
    
    
if __name__ == "__main__":
    # starts the mapper server
    serve(ip="localhost", port="7000", max_workers = 10)