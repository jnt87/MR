#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include <fstream>
#include <sstream>
#include <vector>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::InsecureServerCredentials;

using masterworker::MapTask;
using masterworker::FileShardPiece;
using masterworker::MapReply;
using masterworker::ReduceTask;
using masterworker::ReduceReply;

using std::cout;
using std::endl;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker final : public masterworker::Worker::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr;
		int num_executions = 0;

		// Worker server MapReduce interface
		Status Map(ServerContext* context, const MapTask* request, MapReply* reply) override {
			cout << "Worker at " << ip_addr  << " received a Map task." << endl;
			std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(request->user_id());

			// Create temp files for storing output
			std::vector<std::string> temp_file_names;
			for (int i = 0; i < request->temp_files_num(); i++) {
				std::stringstream temp_file_name;
				// Num executions is appended to prevent file name conflicts
				temp_file_name << ip_addr << "-" << i << "-" << num_executions;
				temp_file_names.push_back(temp_file_name.str());
				reply->add_map_out_files(temp_file_name.str());
			}

			// Run Map
			for (FileShardPiece fs_piece : request->file_shard()) {
				// Open the input file
				std::ifstream input_file(fs_piece.filename());
				if (input_file.good()) {
					cout << "Worker " << ip_addr << " opened " << fs_piece.filename() << endl;
				} else {
					cout << "Worker " << ip_addr << " failed to open " << fs_piece.filename() << endl;
					return Status::CANCELLED; // should probably be more precise
				}
				// Call Map on each line of the input file
				std::string line;
				input_file.seekg(fs_piece.offset());
				// Need to check the output of get line; otherwise, errors in the end of the file 
				while (input_file.tellg() < fs_piece.end() && std::getline(input_file, line)) {
					mapper->map(line);
				}
			}

			// Flush the Map results
			std::hash<std::string> hasher;
			while (!mapper->impl_->map_out_keys.empty()) {
				std::string key = mapper->impl_->map_out_keys.front();
				std::string val = mapper->impl_->map_out_vals.front();
				mapper->impl_->map_out_keys.pop_front();
				mapper->impl_->map_out_vals.pop_front();

				// Use the hash of the key to determine which file to write
				int file_index = hasher(key) % temp_file_names.size();
				std::string temp_file_name = temp_file_names[file_index];
				std::ofstream temp_file(temp_file_name, std::ios::app);
				if (temp_file.is_open()) {
					temp_file << key << " " << val << endl;
				} else {
					cout << "Worker " << ip_addr << " failed to open a temp file." << endl;
					return Status::CANCELLED; // should probably be more precise
				}
			}

			cout << "Worker at " << ip_addr  << " compeleted its Map task." << endl;

			num_executions++;
			return Status::OK;
		}
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	ip_addr = ip_addr_port;
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;

	ServerBuilder builder;
	builder.AddListeningPort(ip_addr, InsecureServerCredentials());
	builder.RegisterService(this);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	cout << "Worker listening on " << ip_addr << endl;
	server->Wait();
}
