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

#include <dlfcn.h>

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
		std::unordered_map<std::string, std::vector<int32_t> > variableDimensions;
		std::unordered_map<std::string, double* > localData;
		

	public:
		explicit RemoteComputationKernelServer() {
				
		}
	
	/* reset the state of the kernel */
	Status Reset(ServerContext* context, const ResetRequest* req, ResetResult* res) {
		std::cout << "Reset" << std::endl;

		for (auto i: localData) {
			free(i.second);
		}
		localData.clear();
		variableDimensions.clear();


		res->set_status(0);

		return Status::OK;
	}
	
	Status SendVariable(ServerContext* context, const SendVariableRequest* req, SendVariableResult* res) {
		// receive data from Client

		std::string variableName = req->variablename();

		std::cout << "receive variable '" << variableName << "'" << std::endl;

		RepeatedField<int32_t> dimensions = req->dimensions();
		RepeatedField<double> data = req->data();

		// only up to 2d supported at the moment
		if (dimensions.size()>2) {
			res->set_status(1);
			return Status::OK;
		}
		
		variableDimensions[variableName].resize(dimensions.size());

		std::cout << "allocating ";

		int64_t numTotal = 1;
		int i=0;
		for (int32_t dim: dimensions) {
			variableDimensions[variableName][i] = dim;

			numTotal *= dim;
			
			if (i==0 && dimensions.size()>1) {
				std::cout << dim << "x";
			} else {
				std::cout << dim;
			}
			i++;
		}

		std::cout << " matrix elements: " << numTotal << std::endl;

		double* temp = (double*) malloc(numTotal*sizeof(double));

		i=0;
		for (double d: data) {
			temp[i] = d;
			i++;
		}

		localData[variableName] = temp;
		
		return Status::OK;
	}

	Status GetVariable(ServerContext* context, const GetVariableRequest* req, GetVariableResult* res) {

		std::string queryName = req->variablename();

		if (localData.count(queryName)>0 && variableDimensions.count(queryName)>0) {
			std::vector<int32_t> dimensions = variableDimensions[queryName];
			double* data = localData[queryName];
			
			int64_t numTotal = 1;
			for (int32_t dim: dimensions) {
				res->add_dimensions(dim);
				numTotal *= dim;
			}

			std::cout << "return a total of "<<numTotal << " values for "<<queryName << std::endl;

			for (int i=0; i<numTotal; ++i) {
				res->add_data(data[i]);
			}
			res->set_status(0);
		} else {
			res->set_status(1);
		}

		return Status::OK;
	}

	Status Execute(ServerContext* context, const ExecuteRequest* req, ExecuteResult* res) {
	/*	
		for (int i=0; i<localData["testVariable"].size(); ++i) {
			
			localData["testVariable"][i] += 1.0;

		}

*/
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

