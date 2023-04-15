import os
import grpc
from concurrent import futures
import Project_pb2
import Project_pb2_grpc
import multiprocessing

class MapperServicer(Project_pb2_grpc.MapperServicer):
    def map(self, request, context):
        shardList=request.shards

        print(f"Received {shardList} shards")


        mapWorkers=[]

        for mapperID in range(len(shardList)):
            print(mapperID)

        return Project_pb2.Response(success=True)
    
def serve(host, port):
    Mapper = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Project_pb2_grpc.add_MapperServicer_to_server(MapperServicer(), Mapper)
    Mapper.add_insecure_port(f"{host}:{port}")
    Mapper.start();
    print("Mapper started at {}:{}".format(host, port))
    Mapper.wait_for_termination()

if __name__ == "__main__":
    host = "localhost"
    port = "6000"
    serve(host, port)