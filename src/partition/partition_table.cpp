
#include <cassert>
#include <cstdint>
#include <climits>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

#include "partition_io.h"

#include "partition_table.h"

PartitionTable::PartitionTable(std::string filename)
{
	this->filename = filename;

	partition_to_nodes = readPartitionTable(filename.c_str(), &n_partitions, &n_replicas);
	if (partition_to_nodes == NULL)
	{
		throw "failed to read partition table";
	}
}

PartitionTable::~PartitionTable()
{
	free(partition_to_nodes);
}

partition_t PartitionTable::getPartition(unsigned long hash)
{
	return hash % n_partitions;
}

bool PartitionTable::updatePartitionOwner(node_t old_owner, 
		node_t new_owner, partition_t partition_no)
{
	assert(partition_no < n_partitions);
	int base_index = partition_no * n_replicas;

	bool success = false;

	for (int i = 0; i < n_replicas; i++)
	{
		if (partition_to_nodes[i + base_index] == new_owner)
		{
			success = true;
		}
	}

	for (int i = 0; i < n_replicas; i++)
	{
		if (partition_to_nodes[i + base_index] == old_owner)
		{
			partition_to_nodes[i + base_index] = new_owner; 
			success = true;
		}
	}

	std::string tmp_file = filename + ".tmp";
	if (success) 
	{
		success = writePartitionTable(tmp_file.c_str(), partition_to_nodes, n_partitions, n_replicas);
		if (success) // atomic swap partition files
		{
			//renameat2(AT_FDCWD, tmp_file.c_str(), AT_FDCWD, filename.c_str(), RENAME_EXCHANGE); 
			success = (rename(tmp_file.c_str(), filename.c_str()) == 0);
		}
	}
	
	return success;
}

partition_t *PartitionTable::getPartitionOwners(partition_t partition_no)
{
	assert(partition_no < n_partitions);
	std::list<node_t> ret;
	int base_index = partition_no * n_replicas;
	return &partition_table[base_index];
}

partition_t PartitionTable::getNextPartition(partition_t partition_no)
{
	assert(partition_no < n_partitions);
	partition_no += 1;
	return (partition_no >= n_partitions) ? 0 : partition_no;
}