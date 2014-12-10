#ifndef STATE_H
#define STATE_H

#include <stdint.h>
#include <map>
#include <string>
#include <mutex>

#include "partition/partition_db.h"

#include "util/rw_lock.h"
#include "edisense_types.h"

enum class NodeState
{ 
	JOINING,
	LEAVING,
	STABLE,
};

enum class PartitionState
{
	STABLE,
	RECEIVED,
	RECEIVING,
	DONATING
};

typedef struct PartitionMetadata
{
	PartitionDB *db;
	PartitionState state;
	bool pinned;
	node_t other_node; // If state is MIGRATING_*
} PartitionMetadata;

class NodeStateMachine
{
public:
	transaction_t getTransactionID();

	// use writer lock to modify, reader lock to read
	NodeState state;
	RWLock state_lock;
	
	std::map<partition_t, PartitionMetadata> partitions_owned_map;
	RWLock partitions_owned_map_lock;

	std::map<node_t, std::string> cluster_members;
	RWLock cluster_members_lock;

	void saveNodeState(std::string &filename); // saves counter and state
	void loadNodeState(std::string &filename);
	
	void savePartitionState(std::string &filename); // saves partitions owned by this node
	void loadPartitionState(std::string &filename);

	void saveClusterMemberList(std::string &filename); // saves list of cluster members
	void loadClusterMemberList(std::string &filename);
private:
	transaction_t counter = 0;
	std::recursive_mutex counter_lock;
};

#endif /* STATE_H */