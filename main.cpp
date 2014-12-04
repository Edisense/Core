
#include <cassert>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <climits>
#include <iostream>

//#include <boost/filesystem.hpp>

#include "global.h"
#include "state.h"

#include "server/server_internal.h"

#include "partition/partition_db.h"

#include "daemons/daemons.h"

#include "util/hash.h"

#define NOT_IMPLEMENTED printf("NOT_IMPLEMENTED\n"); exit(0);

static void InitializeState()
{
  g_current_node_state = new NodeStateMachine();

  char hostname[HOST_NAME_MAX + 1]; 
  if (gethostname(hostname, sizeof(hostname)) != 0)
  {
    perror("Unable to gethostname for current machine. Exiting.");
    exit(1);
  }
  g_current_node_id = hostToNodeId(std::string(hostname)); // from hash.h

  // the intial partition map must exist <---------need boost
 // assert(boost::filesystem::exists(boost::filesystem::path(g_cached_partition_map_filename))); 
  g_cached_partition_table = new PartitionTable(g_cached_partition_map_filename);

  // initialize partition table
  int num_partitions = g_cached_partition_table->getNumPartitions();
  int num_replicas = g_cached_partition_table->getNumReplicas();
  node_t *partition_table = g_cached_partition_table->getPartitionTable();
  std::cout << "initialize for " << num_partitions << " partitions and " << num_replicas << " replicas" << std::endl;
  
  // build list of partitions owned from scratch
  for (partition_t partition_id = 0; partition_id < num_partitions; partition_id++)
  {
    int base_index = partition_id * num_replicas;
    for (int i = 0; i < num_replicas; i++)
    {
      if (partition_table[i + base_index] == g_current_node_id)
      {
        PartitionMetadata pm;
        pm.db = new PartitionDB(GetPartitionDBFilename(partition_id));
        pm.state = PartitionState::STABLE;

        g_current_node_state->partitions_owned_map[partition_id] = pm;
      }
    }
  }

  // cluster member list must exist <---------need boost
 // assert(boost::filesystem::exists(boost::filesystem::path(g_cluster_member_list_filename))); 
  g_current_node_state->loadClusterMemberList(g_cluster_member_list_filename);

  g_current_node_state->state = NodeState::STABLE;

  // save the partition state
  g_current_node_state->savePartitionState(g_owned_partition_state_filename);
  g_current_node_state->saveNodeState(g_current_node_state_filename);
}

// Citation: argument parsing from word2vec by T. Mikolov
static int ArgPos(const char *str, int argc, const char **argv) 
{
  int a;
  for (a = 1; a < argc; a++) if (!strcmp(str, argv[a])) 
  {
    if (a == argc - 1) 
    {
      printf("Argument missing for %s\n", str);
      exit(1);
    }
    return a;
  }
  return -1;
}

int main(int argc, const char *argv[])
{
  int i;
  bool join = false, recover = false;
  if (i = ArgPos("--datadir", argc, argv) > 0) 
    g_db_files_dirname = std::string(argv[i+1]);
  if (i = ArgPos("--nodestate", argc, argv) > 0) 
    g_current_node_state_filename = std::string(argv[i+1]);
  if (i = ArgPos("--clustermembers", argc, argv) > 0) 
    g_cluster_member_list_filename = std::string(argv[i+1]);
  if (i = ArgPos("--ownershipmap", argc, argv) > 0)
    g_owned_partition_state_filename = std::string(argv[i+1]);
  if (i = ArgPos("--partitionmap", argc, argv) > 0)
    g_cached_partition_map_filename = std::string(argv[i+1]);
  if (i = ArgPos("--join", argc, argv) > 0)
    join = true;
  if (i = ArgPos("--recover", argc, argv) > 0)
    recover = true;

  if (join && recover)
  {
    perror("cannot join and recover");
    exit(0);
  }
  
  if (g_db_files_dirname == "")
  {
    perror("must specify a directory for database files");
    exit(0);
  }
  if (g_current_node_state_filename == "");
  {
    perror("must specify a filename for persisting node state");
    exit(0);
  }
  if (g_cluster_member_list_filename == "")
  {
    perror("must specify a file for list of cluster members");
    exit(0);
  }
  if (g_owned_partition_state_filename == "")
  {
    perror("must specify a file for owned partition state");
    exit(0);
  }
  if (g_cached_partition_map_filename == "")
  {
    perror("must specify a file for partition map");
    exit(0);
  }

  if (join)
  {
    NOT_IMPLEMENTED
  }
  else if (recover)
  {
    NOT_IMPLEMENTED    
  }
  else
  {
    InitializeState();
  }

  std::thread rebalance_thread(LoadBalanceDaemon, 60 * 5); // 5 minutes
  std::thread gc_thread(GarbageCollectDaemon, 60 * 60 * 12); // 12 hrs
}