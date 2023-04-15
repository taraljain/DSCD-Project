import grpc
import Project_pb2
import Project_pb2_grpc

with grpc.insecure_channel('localhost:5000') as channel:
    stub=Project_pb2_grpc.MasterStub(channel)
    success=stub.InvertedIndex(Project_pb2.Request(inputDataLocation="./InputFiles", numberOfMappers=3, numberOfReducers=2, outputDataLocation="./OutputFiles"))

    print(success)