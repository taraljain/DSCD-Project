import os
import grpc
from concurrent import futures
import Project_pb2
import Project_pb2_grpc

class MasterServicer(Project_pb2_grpc.MasterServicer):
    def InvertedIndex(self, request, context):
        print(f"Client Request : {request}")
        shardList = generateShardInput(request.numberOfMappers, request.inputDataLocation)

        with grpc.insecure_channel('localhost:6000') as channel:
            stub=Project_pb2_grpc.MapperStub(channel)


            shard_data=[Project_pb2.ShardList(shards=data) for data in shardList]

            request_data=Project_pb2.RequestMapper(inputDataLocation=request.inputDataLocation,shardlist = shard_data)
            response=stub.map(request_data)

            print(f"Response from Mapper {response}")

        with grpc.insecure_channel('localhost:7000') as channel:
            stub = Project_pb2_grpc.ReducerStub(channel)
            response = stub.reduce(Project_pb2.RequestReducer(numberOfReducers=request.numberOfReducers, intermediateOutputDataLocation="./IntermediateOutputs", finalOutputDataLocation=request.outputDataLocation))
            print("Response from Reducer: {}".format(response))


        return Project_pb2.Response(success=True)

def generateShardInput(numMappers, inputDir):
    # Get the list of text files in the input directory
    fileList = os.listdir(inputDir)
    numFiles = len(fileList)

    # Compute the number of files to assign to each mapper
    filesPerMapper = numFiles // numMappers
    leftoverFiles = numFiles % numMappers

    # Assign files to each mapper
    shardList = []
    start = 0
    for i in range(numMappers):
        end = start + filesPerMapper
        if i < leftoverFiles:
            end += 1
        
        ith_mapper=[]
        # append file with it's document ID in the list
        for j in range(start,end):
            element=Project_pb2.Shard(document_id=get_document_id(fileList[j]),filename=fileList[j])
            ith_mapper.append(element)
        
        shardList.append(ith_mapper)
        
        start = end
    
    return shardList

def get_document_id(filename):
    data=filename.split(".")[0]

    id=''

    for i in range(len(data)-1,0,-1):
        letter=data[i]

        if letter.isnumeric():
            id=letter+id

        else:
            break
    
    return int(id)

def serve(host, port):
    Master = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Project_pb2_grpc.add_MasterServicer_to_server(MasterServicer(), Master)
    Master.add_insecure_port(f"{host}:{port}")
    Master.start()
    print("Master started at {}:{}".format(host, port))
    Master.wait_for_termination()

if __name__ == "__main__":
    host = "localhost"
    port = "5000"
    serve(host, port)