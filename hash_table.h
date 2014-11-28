#ifndef CONSISTENT_HASH_H
#define CONSISTENT_HASH_H

#include <string>
#include <list>
#include <exception>
#include <pthread.h>

#include "edisense_types.h"

class HashRing 
{
public:
	HashRing(int n_partitions, int n_replicas);
	~HashRing();
	partition_t getPartition(device_t device_id);
	std::list<node_t> getPartitionOwners(partition_t partition_no);
	std::list<node_t> getDeviceOwners(device_t device_id);
	bool updatePartitionOwner(node_t old_owner, 
		node_t new_owner, partition_t partition_no);
	int getNumPartitions() {	return n_partitions;	}
	int getNumReplicas() {	return n_replicas; }
private:
	int n_partitions;
	int n_replicas;
	int n_nodes;

	node_t *partition_to_nodes;
	
	pthread_rwlock_t rw_lock;
};

#endif /* CONSISTENT_HASH_H */