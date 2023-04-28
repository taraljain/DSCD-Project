import grpc
import Project_pb2
import Project_pb2_grpc

'''
# Assumptions:
1) The user provides the input in the desired format (no error handling for the user input)
2) input and output locations will be mentioned w.r.t client.py
3) Assume only two tables, each having only two columns out of which one column is common over which join will happen
4) master process is running beforehead
5) Assume that the same column names (name, age)/(name, role) will be present
'''

def naturalJoin(inputLocation, mappersCount, reducersCount, outputLocation):
    '''
    requests the master process to apply natural join on the input files,
    and write it at the output location,
    returns the status
    '''
    masterAddr = "localhost:6000"
    
    with grpc.insecure_channel(masterAddr) as channel:
        stub = Project_pb2_grpc.MasterStub(channel)
        response = stub.naturalJoin(Project_pb2.MasterRequest(
                        inputLocation = inputLocation,
                        mappersCount =  mappersCount, 
                        reducersCount = reducersCount,
                        outputLocation = outputLocation))

        return response.status
    
    
if __name__ == "__main__":
    print("Welcome in the Natural Join application!")
    
    # user input
    inputLocation = input("Enter the input data location: ")
    mappersCount = int(input("Enter the number of mappers (M): "))
    reducersCount = int(input("Enter the number of reducers (R): "))
    outputLocation = input("Enter the output data location: ")
        
    # calls the master process for the natural join
    status = naturalJoin(inputLocation, mappersCount, reducersCount, outputLocation)
    print(f"Status: {status}")
    
    print("Thank you for using the application!")
