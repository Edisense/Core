
#include <map>
#include <string>
#include <ifstream>
#include <cassert>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>

#include "partition/hash.h" // TODO: how to include this
#include "partition_io.h"

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

    	if (host_to_id.find(host) != host_to_id.end())
    	{
    		cerr << "duplicate hostnames in file: " << host << endl;
       		exit(1);
    	}

    	node_t node_id = hostToNodeId(host);
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

	size_t partition_table_size = n_partitions * n_replicas * sizeof(node_t);

	// allocate partition table
	partition_t partition_table[partition_table_size];

	int partition_table_idx = 0;
	
	bool more_partitions_to_allocate = true;
	while (more_partitions_to_allocate)
	{
		for (auto &kv : host_to_id)
		{
			partition_table[partition_table_idx] = kv.second;
			partition_table_idx++;

			if (partition_table_idx >= partition_table_size)
			{
				more_partitions_to_allocate = false;
				break;
			}
		}
	}

	writePartitionTable(output_file.c_str(), &partition_table, n_partitions, n_replicas);

	cout << "done writing partition_table: " << output_file << endl;
}