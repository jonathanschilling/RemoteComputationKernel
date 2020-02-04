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
#include <stack>

#include <dlfcn.h>

#include <lapacke.h>

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

using rck::AlocVarRequest;
using rck::AlocVarResult;

using rck::FreeVarRequest;
using rck::FreeVarResult;


class RemoteComputationKernelServer : public RemoteComputationKernel::Service {
	
	private:
		int next_id;
		std::deque<int> ids_to_repopulate;

		std::vector<int> vars_dtype;
		std::vector<std::vector<int> > vars_dimensions;
		std::vector<void*> registered_vars;

		int acquire_next_id() {
			int result = -1;
			if (!ids_to_repopulate.empty()) {
				// there is at least one id to re-populate
				result = ids_to_repopulate.front();
				ids_to_repopulate.pop_front();
			} else {
				// no entries in registered_vars to re-populate, so take a fresh one
				result = next_id;
				next_id++;
			}

			// adjust storage of variable-related info
			int currently_used = registered_vars.size();
			if (currently_used == 0) {
				vars_dtype.resize(1);
				vars_dimensions.resize(1);
				registered_vars.resize(1);
			} else if (result >= currently_used) {
				// exponential growth to keep number of resizes small
				vars_dtype.resize(currently_used*2);
				vars_dimensions.resize(currently_used*2);
				registered_vars.resize(currently_used*2);
			}

			return result;
		}

		bool id_in_use(const int& id) {
			if (id>=next_id || std::find(ids_to_repopulate.begin(), ids_to_repopulate.end(), id) != ids_to_repopulate.end()) {
				return false;
			} else {
				return true;
			}
		}

		void free_id(const int& id) {
			ids_to_repopulate.push_back(id);
		}

	public:
		explicit RemoteComputationKernelServer() {
			next_id = 0; // start at the beginning
		}
	
		Status AlocVar(ServerContext* context, const AlocVarRequest* req, AlocVarResult* res) {

			// check given dtype
			int dtype = req->dtype();
			if (dtype==3 || dtype==5 || dtype==6) {
				// given dtype is allowed

				// compute total number of elements
				size_t num_total = 1; // scalar by default
				RepeatedField<int32_t> dims = req->dimensions();
				for (int i=0; i<dims.size(); ++i) {
					num_total *= dims.Get(i);
				}

				// try to allocate memory
				void* target = nullptr;
				if (dtype == 3) { // int
					target = malloc(num_total*sizeof(int));
				} else if (dtype == 5) { // float
					target = malloc(num_total*sizeof(float));
				} else if (dtype == 6) { // double
					target = malloc(num_total*sizeof(double));
				}

				if (target != nullptr) {
					// memory allocation was successful, so actually modify internal state
					int id = acquire_next_id();

					vars_dtype[id] = req->dtype();

					vars_dimensions[id].resize(dims.size());
					for (int i=0; i<dims.size(); ++i) {
						vars_dimensions[id][i] = dims.Get(i);
					}

					registered_vars[id] = target;

					// successful
					res->set_error(0);
					res->set_id(id);
				} else {
					// memory allocation was not successful
					res->set_error(2);
					res->set_id(-1);
				}
			} else {
				// given dtype not recognized
				res->set_error(1);
				res->set_id(-1);
			}

			return Status::OK;
		}


		Status FreeVar(ServerContext* context, const FreeVarRequest* req, FreeVarResult* res) {
			int id = req->id();
			if (id>=0) {
				// id could be used
				if (id_in_use(id)) {
					// id is actually in use
					free(registered_vars[id]);
					free_id(id);
					res->set_error(0);
				} else {
					// id was not in use
					res->set_error(2);
				}
			} else {
				// invalid id given
				res->set_error(1);
			}

			return Status::OK;
		}







	/* reset the state of the kernel */
	Status Reset(ServerContext* context, const ResetRequest* req, ResetResult* res) {
		std::cout << "Reset" << std::endl;


		// free variables

		res->set_error(0);

		return Status::OK;
	}
//
//	Status SendVariable(ServerContext* context, const SendVariableRequest* req, SendVariableResult* res) {
//		// receive data from Client
//
//		std::string variableName = req->variablename();
//
//		std::cout << "receive variable '" << variableName << "'" << std::endl;
//
//		RepeatedField<int32_t> dimensions = req->dimensions();
//		RepeatedField<double> data = req->data();
//
//		// only up to 2d supported at the moment
//		if (dimensions.size()>2) {
//			res->set_status(1);
//			return Status::OK;
//		}
//
//		variableDimensions[variableName].resize(dimensions.size());
//
//		std::cout << "allocating ";
//
//		int64_t numTotal = 1;
//		int i=0;
//		for (int32_t dim: dimensions) {
//			variableDimensions[variableName][i] = dim;
//
//			numTotal *= dim;
//
//			if (i==0 && dimensions.size()>1) {
//				std::cout << dim << "x";
//			} else {
//				std::cout << dim;
//			}
//			i++;
//		}
//
//		std::cout << " matrix elements: " << numTotal << std::endl;
//
//		double* temp = (double*) malloc(numTotal*sizeof(double));
//
//		i=0;
//		for (double d: data) {
//			temp[i] = d;
//			i++;
//		}
//
//		localData[variableName] = temp;
//
//		return Status::OK;
//	}
//
//	Status GetVariable(ServerContext* context, const GetVariableRequest* req, GetVariableResult* res) {
//
//		std::string queryName = req->variablename();
//
//		if (localData.count(queryName)>0 && variableDimensions.count(queryName)>0) {
//			std::vector<int32_t> dimensions = variableDimensions[queryName];
//			double* data = localData[queryName];
//
//			int64_t numTotal = 1;
//			for (int32_t dim: dimensions) {
//				res->add_dimensions(dim);
//				numTotal *= dim;
//			}
//
//			std::cout << "return a total of "<<numTotal << " values for "<<queryName << std::endl;
//
//			for (int i=0; i<numTotal; ++i) {
//				res->add_data(data[i]);
//			}
//			res->set_status(0);
//		} else {
//			res->set_status(1);
//		}
//
//		return Status::OK;
//	}
//
//	Status Execute(ServerContext* context, const ExecuteRequest* req, ExecuteResult* res) {
//
//		// open the library
//		std::cout << "Opening liblapacke.so..." << std::endl;
//		void* handle = dlopen("liblapacke.so", RTLD_LAZY);
//
//		if (!handle) {
//			std::cout << "Cannot open library: " << dlerror() << std::endl;
//			return Status::OK;
//		} else {
//			std::cout << "opened liblapacke.so" << std::endl;
//		}
//
//		// load the symbol
//    std::cout << "Loading symbol LAPACKE_dgeqrf ..." << std::endl;
//    typedef int (*LAPACKE_dgeqrf_t)(int, int, int, double*, int, double*);
//
//		// reset errors
//    dlerror();
//    LAPACKE_dgeqrf_t LAPACKE_dgeqrf = (LAPACKE_dgeqrf_t) dlsym(handle, "LAPACKE_dgeqrf");
//    const char *dlsym_error = dlerror();
//    if (dlsym_error) {
//        std::cout << "Cannot load symbol 'LAPACKE_dgeqrf': " << dlsym_error << std::endl;
//        dlclose(handle);
//        return Status::OK;
//    }
//
//    // use it to do the calculation
//    std::cout << "Calling LAPACKE_dgeqrf..." << std::endl;
//
//		int m=2, n=2, lda=2;
//		double *a = localData["matrix"];
//		double tau[2];
//		int info = LAPACKE_dgeqrf( LAPACK_ROW_MAJOR, m, n, a, lda, tau );
//
//		std::cout << "LAPACKE_dgeqrf result: " << info << std::endl;
//
//    // close the library
//    std::cout << "Closing library..." << std::endl;
//    dlclose(handle);
//
//		return Status::OK;
//	}
};

int main(int argc, char** argv) {
        std::string server_address("127.0.0.1:50051");
        RemoteComputationKernelServer service;

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());
        std::cout << "RemoteComputationKernel listening on " << server_address << std::endl;
        server->Wait();

        return 0;
}

