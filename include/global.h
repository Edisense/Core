#ifndef GLOBAL_H
#define GLOBAL_H

#include <string>

#include "state.h"
#include "partition/partition_table.h"
#include "edisense_types.h"

// cached hash of current node's hostname
extern node_t g_current_node_id;

extern std::string g_current_node_hostname;

// data structure to store partitions owned by this node,
// state of operations, current transaction id, and cluster 
// members list
extern NodeStateMachine *g_current_node_state;

// data structure to store mappings from partition numbers 
// to their owners
extern PartitionTable *g_cached_partition_table;

// maximimum amount of storage that Edisense can use
extern size_t g_local_disk_limit_in_bytes;

// directory containing the db shards
extern std::string g_db_files_dirname;

// file storing state and transaction id
extern std::string g_current_node_state_filename;

// file storing list of all cluster members 
extern std::string g_cluster_member_list_filename;

// file storing list of partitions owned by current node
extern std::string g_owned_partition_state_filename;

// file storing the cached partition map
extern std::string g_cached_partition_map_filename;

#endif /* GLOBAL_H */