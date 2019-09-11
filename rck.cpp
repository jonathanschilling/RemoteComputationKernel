#include <iostream>
#include <sstream>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/credentials.h>

#include "remoteComputationKernel.grpc.pb.h"
#include "remoteComputationKernel.pb.h"

#include <unordered_map>
#include <vector>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using google::protobuf::RepeatedField;

using rck::RemoteComputationKernel;
using rck::ResetRequest;
using rck::ResetResult;
using rck::SendVariableRequest;
using rck::SendVariableResult;
using rck::GetVariableRequest;
using rck::GetVariableResult;
using rck::ExecuteRequest;
using rck::ExecuteResult;

class RemoteComputationKernelServer : public RemoteComputationKernel::Service {
	
	private:
		std::unordered_map<std::string, std::vector<double> > localData;
		

	public:
		explicit RemoteComputationKernelServer() {
			
		}

	Status Reset(ServerContext* context, const ResetRequest* req, ResetResult* res) {
		std::cout << "Reset" << std::endl;

		res->set_status(0);

		return Status::OK;
	}
	
	Status SendVariable(ServerContext* context, const SendVariableRequest* req, SendVariableResult* res) {
		// receive data from Client

		std::string variableName = req->variablename();

		std::cout << "variable '" << variableName << "'" << std::endl;

		RepeatedField<double> data = req->data();
		
		std::vector<double> temp;

		temp.resize(data.size());

		int i=0;
		for (double d: data) {
			std::cout << "data["<<i<<"]="<<d << std::endl;
			temp[i] = d;
			i++;
		}

		localData[variableName] = temp;

		return Status::OK;
	}

	Status GetVariable(ServerContext* context, const GetVariableRequest* req, GetVariableResult* res) {

		std::string queryName = req->variablename();

		if (localData.count(queryName) > 0) {
			std::vector<double> result = localData[queryName]; 
			
			for (double d: result) {
				res->add_data(d);
			}
			res->set_status(0);
		} else {
			res->set_status(1);
		}

		return Status::OK;
	}

	Status Execute(ServerContext* context, const ExecuteRequest* req, ExecuteResult* res) {
		
		for (int i=0; i<localData["testVariable"].size(); ++i) {
			
			localData["testVariable"][i] += 1.0;

		}


		return Status::OK;
	}			
};

int main(int argc, char** argv) {
        std::string server_address("0.0.0.0:50051");
        RemoteComputationKernelServer service;

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "RemoteComputationKernel listening on " << server_address << std::endl;
        server->Wait();

        return 0;
}

