#ifndef STATE_H
#define STATE_H

#include <stdint.h>
#include <map>
#include <string>
#include <mutex>

#include "rw_lock.h"
#include "edisense_types.h"

enum NODE_STATE 
{ 
	JOINING,
	LEAVING,
	STABLE,
	RECOVERING
};

enum PARTITION_STATE
{
	STABLE,
	MIGRATING_FROM,
	MIGRATING_TO
};

struct partition_meta_t
{
	PARTITION_STATE state;
	node_t other_node; // If state is MIGRATING_*
};

class NodeState 
{
public:
	transaction_t getTransactionID(std::string &filename);

	// use writer lock to modify, reader lock to read
	NODE_STATE state;
	RWLock state_lock;
	
	std::map<partition_t, struct partition_meta> partition_map;
	RWLock partition_map_lock;

	std::map<node_t, std::string> cluster_members;
	RWLock cluster_members_lock;

	void saveNodeState(std::string &filename); // saves counter and state
	void loadNodeState(std::string &filename);
	
	void savePartitionState(std::string &filename); // saves partitions owned by this node
	void loadPartitionState(std::string &filename);

	void saveClusterMemberList(std::string &filename); // saves list of cluster members
	void loadClusterMemberList(std::string &filename);
private:
	transaction_t counter;
	std::recursive_mutex counter_lock;
};

#endif /* STATE_H */