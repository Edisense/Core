
#include <cassert>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>

#include "partition_table.h"

// read binary encoded partition table: PARTITION REPLICAS TABLE
static partition_t *readPartitionTable(const char *filename, 
	int *n_partitions, int *n_replicas)
{
	int fd = open(filename, "r");
	if (!fd) 
		return NULL;
	uint32_t partitions, replicas;
	if (read(fd, &partitions, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	if (read(fd, &replicas, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	size_t table_size = sizeof(partition_t) * partitions * replicas;
	partition_t *table = malloc(table_size);
	size_t bytes_read = read(fd, table, table_size);
	if (bytes_read != table_size)
	{
		free(table);
		return NULL;
	}
	*n_partitions = partitions;
	*n_replicas = replicas;
	close(f);
	return table;
}

static void writePartitionTable(const char *filename, partition_t *table, 
	int n_partitions, int n_replicas)
{
	int fd = open(filename, "w");
	assert(fd != 0);
	uint32_t buf = n_partitions;
	assert (write(fd, &buf, sizeof(uint32_t)) == sizeof(uint32_t));
	buf = n_replicas;
	assert (write(fd, &buf, sizeof(uint32_t)) == sizeof(uint32_t));
	size_t table_size = sizeof(partition_t) * n_partitions *n_replicas;
	assert (write(fd, table, table_size) == table_size);
	close(fd);
}

PartitionTable::PartitionTable(std::string filename)
{
	if(pthread_rwlock_init(rw_lock, NULL) != 0)
	{
		throw "failed to init rw lock";
	}

	self.filename = filename;

	partition_to_nodes = readPartitionTable(filename.c_str(), &n_partitions, &n_replicas);
	if (partition_to_nodes == NULL)
	{
		throw "failed to read partition table"
	}
}

PartitionTable::~PartitionTable()
{
	pthread_rwlock_destroy(&rw_lock);
	free(partition_to_nodes);
}

partition_t PartitionTable::getPartition(int hash)
{
	return hash % n_partitions;
}

bool PartitionTable::updatePartitionOwner(node_t old_owner, 
		node_t new_owner, partition_t partition_no);
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
	if (success) 
		writePartitionTable(filename, partition_to_nodes, n_partitions, n_replicas);
	
	return success;
}

partition_t *PartitionTable::getPartitionOwners(partition_t partition_no)
{
	assert(partition_no < n_partitions);
	int base_index = partition_no * n_replicas;
	return &artition_to_nodes[base_index];
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