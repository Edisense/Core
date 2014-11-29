#ifndef PARTITION_TABLE_H
#define PARTITION_TABLE_H

#include <string>
#include <list>
#include <exception>
#include <pthread.h>

#include <edisense_types.h>

class PartitionTable 
{
public:
	PartitionTable(std::string filename);
	~PartitionTable();
	partition_t getPartition(unsigned long hash);

	// must hold reader lock, returns array with partition owners
	partition_t *getPartitionOwners(partition_t partition_no);

	// must hold writer lock
	bool updatePartitionOwner(node_t old_owner, 
		node_t new_owner, partition_t partition_no);
	
	int getNumPartitions() {  return n_partitions;	}
	int getNumReplicas() {	return n_replicas;  }
	partition_t getNextPartition(partition_t partition_no);
	
	void acquireRDLock();
	void releaseRDLock();
	void acquireWRLock();
	void releaseWRLock();
private:
	int n_partitions;
	int n_replicas;
	node_t *partition_to_nodes;
	pthread_rwlock_t rw_lock;
	std::string filename;
};

#endif /* PARTITION_TABLE_H */