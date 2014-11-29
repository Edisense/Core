
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
	if(pthread_rwlock_init(&rw_lock, NULL) != 0)
	{
		throw "failed to init rw lock";
	}

	this->filename = filename;

	partition_to_nodes = readPartitionTable(filename.c_str(), &n_partitions, &n_replicas);
	if (partition_to_nodes == NULL)
	{
		throw "failed to read partition table";
	}
}

PartitionTable::~PartitionTable()
{
	pthread_rwlock_destroy(&rw_lock);
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
	// TODO: should use different filename to stop overwriting
	std::string tmp_file = filename + ".tmp";
	if (success) 
	{
		success = writePartitionTable(tmp_file.c_str(), partition_to_nodes, n_partitions, n_replicas);
		if (success) // atomic swap partition files
		{
			success = renameat2(AT_FDCWD, tmp_file.c_str(), AT_FDCWD, filename.c_str(), RENAME_EXCHANGE); 
		}
	}
	
	return success;
}

partition_t *PartitionTable::getPartitionOwners(partition_t partition_no)
{
	assert(partition_no < n_partitions);
	int base_index = partition_no * n_replicas;
	return &partition_to_nodes[base_index];
}

partition_t PartitionTable::getNextPartition(partition_t partition_no)
{
	assert(partition_no < n_partitions);
	partition_no += 1;
	return (partition_no >= n_partitions) ? 0 : partition_no;
}

void PartitionTable::acquireRDLock()
{
	assert(pthread_rwlock_rdlock(&rw_lock) == 0);
}

void PartitionTable::releaseRDLock()
{
	assert(pthread_rwlock_unlock(&rw_lock) == 0);
}

void PartitionTable::acquireWRLock()
{
	assert(pthread_rwlock_wrlock(&rw_lock) == 0);
}

void PartitionTable::releaseWRLock()
{
	assert(pthread_rwlock_unlock(&rw_lock) == 0);
}
