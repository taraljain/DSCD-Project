import os
import grpc
from concurrent import futures
import Project_pb2
import Project_pb2_grpc
import multiprocessing

class MapperServicer(Project_pb2_grpc.MapperServicer):
    def map(self, request, context):
        shardList=request.shardlist
        inputLocation=request.inputDataLocation

        #print(f"Received {shardList} shards",inputLocation)

        mapWorkers=[]

        for mapperID in range(len(shardList)):
            shardContents=getShardContents(inputLocation,shardList[mapperID].shards)
            
            mapWorkers.append(multiprocessing.Process(target=mapper,args=(mapperID, shardContents)))

        for mapWorker in mapWorkers:
            mapWorker.start()

        for mapWorker in mapWorkers:
            mapWorker.join()

        return Project_pb2.Response(success=True)

# Function to read the content of shard and return a list of [filename, content]
def getShardContents(inputDir,shardList):
    
    shardContents=[]
    for element in shardList:
        document_id=element.document_id
        filename=element.filename

        with open(os.path.join(inputDir,filename),'r') as f:
            content=f.read()
            shardContents.append([document_id,filename,content])
    
    return shardContents

# function which emit (word,document_id)
def mapper(mapperID,shardContents):
    emission=[]

    for content in shardContents:
        for word in content[2].split():
            emission.append([word.lower(),content[0]])
    
    saveIntermediateOutput(mapperID,emission)

    return emission

def saveIntermediateOutput(mapperID, outputList):
    # Save the intermediate output to the intermediate output directory
    with open(f'IntermediateOutputs/{mapperID}.txt', "w") as f:
        for sublist in outputList:
            line = " ".join(str(item) for item in sublist)
            f.write(line + "\n")

def serve(host, port):
    Mapper = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Project_pb2_grpc.add_MapperServicer_to_server(MapperServicer(), Mapper)
    Mapper.add_insecure_port(f"{host}:{port}")
    Mapper.start()
    print("Mapper started at {}:{}".format(host, port))
    Mapper.wait_for_termination()

if __name__ == "__main__":
    host = "localhost"
    port = "6000"
    serve(host, port)