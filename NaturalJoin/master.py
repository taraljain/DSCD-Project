from concurrent import futures
import os
from urllib.parse import urlparse

import grpc
import Project_pb2
import Project_pb2_grpc

'''
# Assumptions:
1) port 6000, 7000, and 8000 are assumed to be free
'''

class MasterServicer(Project_pb2_grpc.MasterServicer):   
    def splitInput(self, inputLocation, mappersCount):
        '''
        parameters:
        inputLocation: root directory of the input files
        mapperCount: number of mappers
        
        walks through the input location,
        append each file in a list containing the shards corresponding to each mapper,
        returns the shards
        '''
        shards_list = [[] for _ in range(mappersCount)]
        fileId = 0
        mapperId = 0
        for subdir, dirs, files in os.walk(inputLocation):     
            for file in files:
                if "table1" not in file:
                    continue
            
                fileLocation1 = os.path.join(subdir, file)
                fileLocation2 = fileLocation1.replace("table1", "table2")
                shards_list[(fileId//2)%mappersCount].append(Project_pb2.File(id = fileId, 
                            location = fileLocation1))
                fileId += 1
                shards_list[(fileId//2)%mappersCount].append(Project_pb2.File(id = fileId, 
                            location = fileLocation2))
                fileId += 1
                
        shards = [Project_pb2.Shard(files = shards_list[i]) for i in range(mappersCount)]
        return shards
    
    
    def naturalJoin(self, request, context):
        '''        
        walks through the input location,
        append each file in a list containing the shards corresponding to each mapper,
        returns the shards
        '''
        mapperAddr = "localhost:7000"
        reducerAddr = "localhost:8000"
        
        clientAddress = urlparse(context.peer()).path
        idx = clientAddress.rfind(':')
        # ip = clientAddress[:idx]
        ip = "localhost"
        port = clientAddress[idx+ 1:]
        print(f"NATURAL JOIN REQUEST FROM {ip}:{port}")
        
        # client request
        inputLocation = request.inputLocation
        mappersCount = request.mappersCount
        reducersCount = request.reducersCount
        outputLocation = request.outputLocation
        
        # step 1: split the input into multiple shards
        shards = self.splitInput(inputLocation, mappersCount)
        
        # phase 1: mapper
        with grpc.insecure_channel(mapperAddr) as channel:
            stub = Project_pb2_grpc.MapperStub(channel)
            response = stub.mapper(Project_pb2.MapperRequest(
                                shards = shards, 
                                mappersCount = mappersCount,
                                reducersCount = reducersCount))
        intermediateLocations = response.intermediateLocations
            
        # phase2: reducer
        with grpc.insecure_channel(reducerAddr) as channel:
            stub = Project_pb2_grpc.ReducerStub(channel)
            response = stub.reducer(Project_pb2.ReducerRequest(
                                intermediateLocations = intermediateLocations,
                                outputLocation = outputLocation,
                                reducersCount = reducersCount))
        status = response.status
        
        # step 7: return to the user
        return Project_pb2.MasterResponse(status=status)

    
def serve(ip, port, max_workers):
    '''
    parameters:
    ip, port: ip, port on which the master server will aceept the requests
    max_workers: size of the thread pool to be used by the server to execute RPC handlers
    
    starts the master server on the specified port,
    waits for the client requests
    '''
    # starts the mapper and reducer programs
    os.system("start cmd /K python mapper.py")
    os.system("start cmd /K python reducer.py")
        
    # creates a server with Master service on the specified port
    masterServer = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    Project_pb2_grpc.add_MasterServicer_to_server(MasterServicer(), masterServer)
    masterServer.add_insecure_port(f"{ip}:{port}")
    
    # starts the server
    masterServer.start()
    print(f"Master Server running on {ip}:{port}")
    masterServer.wait_for_termination()
    
    
if __name__ == "__main__":
    # starts the master server
    serve(ip="localhost", port="6000", max_workers = 10)
