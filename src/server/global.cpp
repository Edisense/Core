#include "global.h"

// Id of current node -- computed from hash of hostname
node_t g_current_node_id;

// State of current node
NodeStateMachine *g_current_node_state;

// Mapping of partitions to owners
PartitionTable *g_cached_partition_table;

size_t g_local_disk_limit_in_bytes;

// Files used by Edisense

std::string g_db_files_dirname;

std::string g_current_node_state_filename;

std::string g_cluster_member_list_filename;

std::string g_owned_partition_state_filename;

std::string g_cached_partition_map_filename;

std::string g_load_rebalance_daemon_log_filename;