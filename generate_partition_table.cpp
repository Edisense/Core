
#include <map>
#include <string>
#include <cassert>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <iostream>

#include "util/hash.h"
#include "partition/partition_io.h"

#include "edisense_types.h"

using namespace std;

static map<string, node_t> readHostsFile(string &hostnames)
{
	map<string, node_t> host_to_id;

	ifstream ifs(hostnames);
	if (!ifs)
    {
        cerr << hostnames << " could not be opened for reading!" << endl;
        exit(1);
    }

    while(ifs) // host file is whitespace delimited
    {
    	string host;
    	ifs >> host;

    	if (ifs.fail()) break;

    	if (host_to_id.find(host) != host_to_id.end())
    	{
    		cerr << "duplicate hostnames in file: " << host << endl;
       		exit(1);
    	}

    	node_t node_id = hostToNodeId(host);

    	cout << host << '\t' << node_id << endl;

    	host_to_id[host] = node_id;
    }

	ifs.close(); 
    return host_to_id;
}

int main(int argc, char *argv[])
{
	if (argc != 5)
	{
		cerr << "Usage: ./generate_partition_table <hostnames file> <output file> <n_partitions> <n_replicas>" << endl; 
		return 0;
	}

	string hostnames(argv[1]);
	string output_file(argv[2]);

	int n_partitions = atoi(argv[3]);
	int n_replicas = atoi(argv[4]);
	// cerr << n_partitions << endl;
	// cerr << n_replicas << endl;
	if (n_partitions <= 0 || n_replicas <= 0) // TODO: determine reasonable limits
	{
		cerr << "thats not cool... must set reasonable number of replacas and partitions" << endl; 
		return 0;
	}

	map<string, node_t> host_to_id = readHostsFile(hostnames);
	int n_hosts = host_to_id.size();

	if (n_hosts < n_replicas)
	{
		cerr << "there cannot be more replicas than hosts" << endl; 
		return 0;
	}

	size_t partition_table_size = n_partitions * n_replicas;
	cerr << "partition table size: " << partition_table_size << endl;

	// allocate partition table
	node_t partition_table[partition_table_size];

	int partition_table_idx = 0;
	
	bool more_partitions_to_allocate = true;
	while (more_partitions_to_allocate)
	{
		for (auto &kv : host_to_id)
		{
			cerr << "allocate: (" << partition_table_idx << ") " << kv.second << endl;
			partition_table[partition_table_idx] = kv.second;
			partition_table_idx++;

			if (partition_table_idx >= partition_table_size)
			{
				more_partitions_to_allocate = false;
				break;
			}
		}
	}

	assert(writePartitionTable(output_file.c_str(), partition_table, n_partitions, n_replicas));

	// sanity check
	int replicas;
	int partitions;
	node_t *partition_table2 = readPartitionTable(output_file.c_str(), &partitions, &replicas);
	assert(replicas == n_replicas);
	assert(partitions == n_partitions);
	for (int i = 0; i < partition_table_size; i++) 
		assert(partition_table[i] == partition_table2[i]);

	cout << "done writing partition_table: " << output_file << endl;
}