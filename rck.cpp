#include <iostream>
#include <sstream>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/credentials.h>

#include "remoteComputationKernel.grpc.pb.h"
#include "remoteComputationKernel.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

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
	public:
		explicit RemoteComputationKernelServer() { }

	Status Reset(ServerContext* context, const ResetRequest* req, ResetResult* res) {
		return Status::OK;
	}
	
	Status SendVariable(ServerContext* context, const SendVariableRequest* req, SendVariableResult* res) {
		return Status::OK;
	}

	Status GetVariable(ServerContext* context, const GetVariableRequest* req, GetVariableResult* res) {
		return Status::OK;
	}

	Status Execute(ServerContext* context, const ExecuteRequest* req, ExecuteResult* res) {
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

