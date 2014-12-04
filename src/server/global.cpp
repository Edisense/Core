#include "global.h"

node_t g_current_node_id;

NodeStateMachine *g_current_node_state;

PartitionTable *g_cached_partition_table;

size_t g_local_disk_limit_in_bytes;

std::string g_db_files_dirname;

std::string g_current_node_state_filename;

std::string g_cluster_member_list_filename;

std::string g_owned_partition_state_filename;

std::string g_cached_partition_map_filename;

std::string g_load_rebalance_daemon_log_filename;