#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <fstream>
#include <math.h>

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShardPiece {
	std::string filename;
	int offset;
	int end;
};

struct FileShard {
	std::vector<FileShardPiece*> pieces;
	int shard_id;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	cout << "starting file sharding" << std::endl;
	int total_size;
	FileShard * shard = new FileShard;
	FileShardPiece * fsp = new FileShardPiece;
	int bytes_used = 0;
	int shard_id = 0;
	int size;
	int curr_offset;
	int map_size = mr_spec.map_kilobytes * 1024;
	int map_remaining;
	int file_remaining;
	int seek_bound;
	char * c;
	int num_bytes;
        int line_buff = 32;
	for(auto input_file : mr_spec.input_files) {
		std::ifstream file(input_file, std::ios::ate | std::ios::binary); //open and seek to end
		file.seekg(0, std::ios::end); //jump to end
		size = file.tellg(); // record current position as size
		file.seekg(0, std::ios::beg); //jump back to beginning
		curr_offset = 0;
		char ch;
		while(!file.eof()) {
			file.seekg(curr_offset);
			map_remaining = (map_size - bytes_used);
			file_remaining = (size) - curr_offset;
			
			seek_bound = fmin(map_remaining - line_buff, file_remaining);
			file.seekg(curr_offset + seek_bound);
			fsp->filename = input_file;
			fsp->offset = curr_offset;

			if(seek_bound != file_remaining) {
				c = (char *) malloc(1);
				num_bytes = 0;
				while(*c != '\n') {
					file.read(c, 1);
					num_bytes++;
				}
				curr_offset += num_bytes;
			}
			fsp->end = file.tellg();
			shard->pieces.push_back(fsp);
			curr_offset += seek_bound;
			fsp = new FileShardPiece;
			bytes_used += seek_bound;

			if(bytes_used >= ceil(map_size * .99)) {
				cout << "adding shard to list" << std::endl;
				shard->shard_id = shard_id;
				fileShards.push_back(*shard);
				shard = new FileShard;
				shard_id++;
				bytes_used = 0;
			}
			file >> ch; // seekg clears EOF state of stream https://stackoverflow.com/questions/35628288/seekg-does-not-set-eofbit-when-reaching-eof-of-a-stream-is-it-by-design
		}
		total_size += size;
	}
	shard->shard_id = shard_id;

	fileShards.push_back(*shard);
	
	//auto num_shards = ceil(total_size/map_size);
	//cout << "number of shards : " << num_shards << std::endl;
	return true;
	
	

}
