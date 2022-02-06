#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

using masterworker::MapTask;
using masterworker::MapReply;
using masterworker::ReduceTask;
using masterworker::ReduceReply;
using masterworker::Worker;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::InsecureChannelCredentials;
using grpc::Status;

using std::cout;
using std::endl;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec map_reduce_data;
		std::vector<FileShard> file_shards;
		std::map<int, std::unique_ptr<Worker::Stub>> worker_stubs;
		std::vector<int> worker_pool;
		CompletionQueue cq;

		// Keeps track of the data for sent tasks. Works both for Map and Reduce replies
		template <class T>
		struct request_data {
			T reply;
			Status status;
			int port;
		};

		std::map<int, request_data<MapReply>> outstanding_map_requests;
		std::map<int, request_data<ReduceReply>> outstanding_reduce_requests;

		void receive_map_reply() {
			// Block on the completion queue waiting for a worker to respond
			void* tag;
			bool ok = false;
			GPR_ASSERT(cq.Next(&tag, &ok));
			GPR_ASSERT(ok);
			// Identify worker by the port which serves as an Id
			int* worker_port = static_cast<int*>(tag);
			request_data<MapReply> map_data = outstanding_map_requests[*worker_port];
			
			// If the request failed, assume that the worker is dead. It will not 
			// be added to the pool and consequently never be used again.
			if (map_data.status.ok()) {
				cout << "Master received a map reply from worker " << *worker_port << endl;
				worker_pool.push_back(*worker_port);
				outstanding_map_requests.erase(*worker_port);
			} else {
				cout << "Worker " << *worker_port << " is dead: " << endl;
				cout << map_data.status.error_code() << " - " << map_data.status.error_message() << " - " << map_data.status.error_details() << endl;
				// TODO: send the task to a different worker. 
			}
		}

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) :
	map_reduce_data(mr_spec), file_shards(file_shards) {
		cout << "Master initializing" << endl;
		// Populate stubs for each worker and the worker to the pool
		for (int port : map_reduce_data.worker_ip_addr_ports) {
			std::string ip_address = "localhost:" + std::to_string(port);
			std::shared_ptr<Channel> channel = CreateChannel(ip_address, InsecureChannelCredentials());
			worker_stubs.insert(std::make_pair(port, Worker::NewStub(channel)));
			worker_pool.push_back(port);
		}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// Form MapTask message for each shard
	cout << "Master has " << file_shards.size() << " map tasks (shards)" << endl;
	for (FileShard file_shard : file_shards) {
		MapTask request;
		for (FileShardPiece *file_shard_piece : file_shard.pieces) {
			masterworker::FileShardPiece *fs_piece_request = request.add_file_shard();
			fs_piece_request->set_filename(file_shard_piece->filename);
			fs_piece_request->set_offset(file_shard_piece->offset);
			fs_piece_request->set_end(file_shard_piece->end);
		}

		// Set the number of output files for each map task to the number
		// of output files, so that each worker get an equal number of files
		// in reduce stage.
		request.set_temp_files_num(map_reduce_data.n_output_files);
		request.set_user_id(map_reduce_data.user_id);

		// If there are no free workers, wait until one is free
		while (worker_pool.size() == 0) {
			receive_map_reply();
		}

		// Get a free worker
		int free_worker_port = worker_pool.back();
		worker_pool.pop_back();

		// Start async request and add it to the queue
		request_data<MapReply> map_data {
			MapReply(),
			Status(),
			free_worker_port
		};
		outstanding_map_requests.insert(std::make_pair(free_worker_port, map_data));
		// TODO: set context deadline set_deadline()
		ClientContext context;
		std::unique_ptr<ClientAsyncResponseReader<MapReply>> rpc(
			worker_stubs[free_worker_port]->PrepareAsyncMap(&context, request, &cq)
		);
		rpc->StartCall();
		rpc->Finish(&outstanding_map_requests[free_worker_port].reply,
					&outstanding_map_requests[free_worker_port].status, 
					&outstanding_map_requests[free_worker_port].port);
		cout << "Master sent a map task to worker " << free_worker_port << endl;
	}

	// Wait for all mapping tasks to finish
	while (outstanding_map_requests.size() > 0) {
		receive_map_reply();
	}

	return true;

}