#ifndef GLOBAL_H
#define GLOBAL_H

#include <string>

#include "state.h"
#include "partition_table.h"

extern NodeStateMachine g_current_node_state;

extern PartitionTable g_cached_partition_table;

// file storing state and transaction id
extern std::string g_current_node_state_filename;

// file storing list of all cluster members 
extern std::string g_cluster_member_list_filename;

// file storing list of partitions owned by current node
extern std::string g_owned_partition_state_filename;

// file storing the cached partition map
extern std::string g_cached_partition_map_filename;

#endif /* GLOBAL_H */