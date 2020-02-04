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

using rck::SendIntRequest;
using rck::SendIntResult;

using rck::SendFltRequest;
using rck::SendFltResult;

using rck::SendDblRequest;
using rck::SendDblResult;

using rck::RecvIntRequest;
using rck::RecvIntResult;

using rck::RecvFltRequest;
using rck::RecvFltResult;

using rck::RecvDblRequest;
using rck::RecvDblResult;

class RemoteComputationKernelServer : public RemoteComputationKernel::Service {

private:
	int next_id;
	std::deque<int> ids_to_repopulate;

	std::vector<int> vars_dtype;
	std::vector<std::vector<int> > vars_dimensions;
	std::vector<void*> var_ptrs;

	// acquire a new id for the next variable
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
		int currently_available = var_ptrs.size();
		if (currently_available == 0) {
			vars_dtype.resize(1);
			vars_dimensions.resize(1);
			var_ptrs.resize(1);
		} else if (result >= currently_available) {
			// exponential growth to keep number of resize() operations small
			vars_dtype.resize(currently_available*2);
			vars_dimensions.resize(currently_available*2);
			var_ptrs.resize(currently_available*2);
		}

		// initialize new contents to known values
		for (int i=currently_available; i<var_ptrs.size(); ++i) {
			vars_dtype[i] = 0;
			vars_dimensions[i].clear();
			var_ptrs[i] = nullptr;
		}

		return result;
	}

	// figure out if a given variable id is already in use or not
	bool id_in_use(const int& id) {
		if (id>=next_id || std::find(ids_to_repopulate.begin(), ids_to_repopulate.end(), id) != ids_to_repopulate.end()) {
			return false;
		} else {
			return true;
		}
	}

	// make an id in use free to be re-used
	// only call after free(var_ptrs[id]) has been called!
	void free_id(const int& id) {
		ids_to_repopulate.push_back(id);
		vars_dtype[id] = 0;
		vars_dimensions[id].clear();
		var_ptrs[id] = nullptr;
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

				var_ptrs[id] = target;

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
				free(var_ptrs[id]);
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

		// free variables and any history of them
		for (int i=0; i<var_ptrs.size(); ++i) {
			if (var_ptrs[i] != nullptr) {
				free(var_ptrs[i]);
			}
		}
		vars_dtype.clear();
		vars_dimensions.clear();
		var_ptrs.clear();

		next_id = 0;
		ids_to_repopulate.clear();

		res->set_error(0);

		return Status::OK;
	}


	Status SendInt(ServerContext* context, const SendIntRequest* req, SendIntResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches int
			if (vars_dtype[id] == 3) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					num_total *= vars_dimensions[id][i];
				}

				// copy over the data
				RepeatedField<int> data = req->data();
				if (data.size() == num_total) {
					for (int i=0; i<num_total; ++i) {
						((int*)var_ptrs[id])[i] = data.Get(i);
					}
					res->set_error(0);
				} else {
					// number of elements does not match
					res->set_error(3);
				}

			} else {
				// dtype of variable is not int
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}

	Status SendFlt(ServerContext* context, const SendFltRequest* req, SendFltResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches float
			if (vars_dtype[id] == 5) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					num_total *= vars_dimensions[id][i];
				}

				// copy over the data
				RepeatedField<float> data = req->data();
				if (data.size() == num_total) {
					for (int i=0; i<num_total; ++i) {
						((float*)var_ptrs[id])[i] = data.Get(i);
					}
					res->set_error(0);
				} else {
					// number of elements does not match
					res->set_error(3);
				}

			} else {
				// dtype of variable is not float
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}

	Status SendDbl(ServerContext* context, const SendDblRequest* req, SendDblResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches double
			if (vars_dtype[id] == 6) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					num_total *= vars_dimensions[id][i];
				}

				// copy over the data
				RepeatedField<double> data = req->data();
				if (data.size() == num_total) {
					for (int i=0; i<num_total; ++i) {
						((double*)var_ptrs[id])[i] = data.Get(i);
					}
					res->set_error(0);
				} else {
					// number of elements does not match
					res->set_error(3);
				}

			} else {
				// dtype of variable is not double
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}

	Status RecvInt(ServerContext* context, const RecvIntRequest* req, RecvIntResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches int
			if (vars_dtype[id] == 3) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					res->add_dimensions(vars_dimensions[id][i]);
					num_total *= vars_dimensions[id][i];
				}

				// actually send data
				for (int i=0; i<num_total; ++i) {
					res->add_data(((int*)var_ptrs[id])[i]);
				}

				res->set_error(0);

			} else {
				// dtype of variable is not int
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}

	Status RecvFlt(ServerContext* context, const RecvFltRequest* req, RecvFltResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches float
			if (vars_dtype[id] == 5) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					res->add_dimensions(vars_dimensions[id][i]);
					num_total *= vars_dimensions[id][i];
				}

				// actually send data
				for (int i=0; i<num_total; ++i) {
					res->add_data(((float*)var_ptrs[id])[i]);
				}

				res->set_error(0);

			} else {
				// dtype of variable is not float
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}

	Status RecvDbl(ServerContext* context, const RecvDblRequest* req, RecvDblResult* res) {
		int id = req->id();

		// verify that the given id is actually registered
		if (id_in_use(id)) {

			// check if dtype matches double
			if (vars_dtype[id] == 6) {

				// compute total number of elements in allocated array
				int num_total = 1;
				for (int i=0; i<vars_dimensions[id].size(); ++i) {
					res->add_dimensions(vars_dimensions[id][i]);
					num_total *= vars_dimensions[id][i];
				}

				// actually send data
				for (int i=0; i<num_total; ++i) {
					res->add_data(((double*)var_ptrs[id])[i]);
				}

				res->set_error(0);

			} else {
				// dtype of variable is not float
				res->set_error(2);
			}
		} else {
			// given id not in use
			res->set_error(1);
		}

		return Status::OK;
	}






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

