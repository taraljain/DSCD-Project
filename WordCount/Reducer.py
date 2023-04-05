import os
import grpc
from concurrent import futures
import Project_pb2
import Project_pb2_grpc
import threading


class ReducerServicer(Project_pb2_grpc.ReducerServicer):
    def reduce(self, request, context):
        print("Client Request: {}".format(request))
        
        # Read the intermediate output files
        mapperEmissions = []
        for filename in os.listdir(request.intermediateOutputDataLocation):
            with open(os.path.join(request.intermediateOutputDataLocation, filename), 'r') as f:
                mapperEmissions.append([[item for item in line.split()] for line in f.readlines()])

        numberOfPartitions = request.numberOfReducers

        partitionsData = {}
        for mapperID in range(len(mapperEmissions)):
            for data in mapperEmissions[mapperID]:
                partitionNumber = getPartitionNumber(data[0], numberOfPartitions)
                partitionsData.setdefault(partitionNumber, {}).setdefault(mapperID, []).append(data)

        reducerWorkers = []
        for partitionNumber, partitionData in partitionsData.items():
            reducerWorkers.append(threading.Thread(target=reducer, args=(partitionNumber, partitionData, request.finalOutputDataLocation)))

        for reducerWorker in reducerWorkers:
            reducerWorker.start()

        for reducerWorker in reducerWorkers:
            reducerWorker.join()

        return Project_pb2.Response(success=True)
    

def saveFinalOutput(finalOutputDataLocation, reducerID, outputList):
    # Save the intermediate output to the intermediate output directory
    with open(f'{finalOutputDataLocation}/{reducerID}.txt', "w") as f:
        for sublist in outputList:
            line = " ".join(str(item) for item in sublist)
            f.write(line + "\n")


def reducer(partitionNumber, partitionData, finalOutputDataLocation):
    sortedData = getSortedData(partitionData)

    reducedOutput = []
    for wordValues in sortedData: 
        reducedOutput.append([wordValues[0], sum(int(item) for item in wordValues[1])])

    saveFinalOutput(finalOutputDataLocation, partitionNumber, reducedOutput)


def getSortedData(partitionData):
    sortedData = []
    temp = []
    for values in partitionData.values():
        for item in values:
            found = False
            for i in range(len(temp)):
                if item[0] == temp[i][0]:
                    temp[i][1].append(item[1])
                    found = True
                    break
            if not found:
                temp.append([item[0], [item[1]]])
        sortedData = temp
        sortedData.sort(key=lambda x: x[0])

    return sortedData


def getPartitionNumber(key, numberOfPartitions):
    return len(key) % numberOfPartitions


def serve(host, port):
    Reducer = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Project_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(), Reducer)
    Reducer.add_insecure_port(f"{host}:{port}")
    Reducer.start();
    print("Reducer started at {}:{}".format(host, port))
    Reducer.wait_for_termination()

if __name__ == "__main__":
    host = "localhost"
    port = "7000"
    serve(host, port)
    