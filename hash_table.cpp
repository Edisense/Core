
#include "consistent_hash.h"

HashRing::HashRing(int n_partitions, int n_replicas)
{
	if(pthread_rwlock_init(rw_lock, NULL) != 0)
	{
		throw "failed to init rw lock";
	}

	self.n_partitions = n_partitions;
	self.n_replicas = n_replicas;

	partition_to_nodes = calloc(n_partitions * n_replicas, sizeof(node_t));
}

HashRing::~HashRing()
{
	pthread_rwlock_destroy(&rw_lock);
	free(partition_to_nodes);
}

HashRing::getPartition(device_t device_id);