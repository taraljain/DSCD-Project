import os
import grpc
from concurrent import futures
import Project_pb2
import Project_pb2_grpc
import multiprocessing

class ReducerServicer(Project_pb2_grpc.ReducerServicer):
    def reduce(self, request, context):
        print("Client Request: {}".format(request))

        # Read the intermediate output files
        mapperEmissions = []
        for filename in os.listdir(request.intermediateOutputDataLocation):
            with open(os.path.join(request.intermediateOutputDataLocation, filename), 'r') as f:
                mapperEmissions.append([[item for item in line.split()] for line in f.readlines()])

        numberOfReducers = request.numberOfReducers
        parition_data=[[[] for _ in range(0,len(mapperEmissions))] for i in range(0,numberOfReducers)]

        for i in range(0,len(mapperEmissions)):
            for word_data in mapperEmissions[i]:
                parition_index=len(word_data[0])%numberOfReducers

                parition_data[parition_index][i].append(word_data)
        

        reducerWorkers=[]

        for reducer_i in range(0,len(parition_data)):
            data=parition_data[reducer_i]

            reducerWorkers.append(multiprocessing.Process(target=reducer,args=(data,reducer_i,request.finalOutputDataLocation)))

        for reducerWorker in reducerWorkers:
            reducerWorker.start()

        for reducerWorker in reducerWorkers:
            reducerWorker.join()

        return Project_pb2.Response(success=True)


def sort_data(data):
    sorted_data=[]
    for data_list in data:
        temp=sorted(data_list,key=lambda x: (x[0]))

        sorted_data.append(temp)
    
    return sorted_data

def saveFinalOutput(reduced_output,output_location,reducerID):
    # Save the intermediate output to the intermediate output directory
    with open(f'{output_location}/{reducerID}.txt', "w") as f:
        for word_key in reduced_output:
            documents_list=list(reduced_output[word_key])
            documents_list.sort()

            line=word_key+' '

            for document_id in documents_list:
                line+=str(document_id)+', '

            f.write(line + "\n")

def reducer(data,reducerID,output_location):
    sorted_data=sort_data(data)

    reduced_output={}

    for intermediate_data in sorted_data:
        for word in intermediate_data:
            if word[0] not in reduced_output:
                reduced_output[word[0]]=set()
            
            reduced_output[word[0]].add(int(word[1]))
    
    saveFinalOutput(reduced_output,output_location,reducerID)


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
        

# [
#     [
#         [['this', '1'], ['is', '1'], ['an', '1'], ['is', '1'], ['in', '1']], 
#         [['be', '2'], ['afraid', '2'], ['of', '2'], ['some', '2'], ['born', '2'], ['some', '2'], ['others', '2'], ['have', '2'], ['thrust', '2'], ['upon', '2'], ['them', '2'], ['work', '5'], ['is', '5'], ['is', '5'], ['is', '5'], ['work', '5'], ['is', '5']], 
#         [['work', '4'], ['is', '4'], ['is', '4'], ['is', '4'], ['work', '4'], ['is', '4'], ['work', '3'], ['is', '3'], ['is', '3'], ['is', '3'], ['work', '3'], ['is', '3']]
#     ], 

#     [
#         [['apple', '1'], ['apple', '1'], ['red', '1'], ['color', '1']], 
#         [['not', '2'], ['greatness', '2'], ['are', '2'], ['great', '2'], ['achieve', '2'], ['greatness', '2'], ['and', '2'], ['greatness', '2'], ['worship', '5'], ['worship', '5'], ['faith', '5'], ['faith', '5'], ['faith', '5'], ['not', '5'], ['faith', '5']], 
#         [['worship', '4'], ['worship', '4'], ['faith', '4'], ['faith', '4'], ['faith', '4'], ['not', '4'], ['faith', '4'], ['worship', '3'], ['worship', '3'], ['faith', '3'], ['faith', '3'], ['faith', '3'], ['not', '3'], ['faith', '3']]
#     ]
# ]