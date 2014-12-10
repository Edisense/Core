
#include <cassert>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <climits>
#include <iostream>
#include <edisense_comms.h>
#include <member.h>
#include <sys/stat.h>

//#include <boost/filesystem.hpp>

#include "global.h"
#include "state.h"

#include "server/server_internal.h"
#include "partition/partition_db.h"
#include "daemons/daemons.h"
#include "util/hash.h"
#include "ble/ble_client_internal.h"

#define NOT_IMPLEMENTED printf("NOT_IMPLEMENTED\n"); exit(0);

#ifndef HOST_NAME_MAX // This variable isn't present in BSD, which means it isn't on OSX either
#define HOST_NAME_MAX 255
#endif

#define DEBUG(x) printf("%d\n", x);

static void InitializeState()
{
  g_current_node_state = new NodeStateMachine();

  char hostname[HOST_NAME_MAX + 1]; 
  if (gethostname(hostname, sizeof(hostname)) != 0)
  {
    perror("Unable to gethostname for current machine. Exiting.");
    exit(1);
  }
  // print current machine's hostname and id
  std::cout << "Machine hostname : " << hostname << std::endl;
  g_current_node_id = hostToNodeId(std::string(hostname)); // from hash.h
  std::cout << "Machine node id : " << g_current_node_id << std::endl;

  // database direcory must exist
  struct stat stat_database_dir;;
  if (lstat(g_db_files_dirname.c_str(), &stat_database_dir) == -1) 
  {
    std::cout << "DB directory does not exist. Creating it" << std::endl;
    mkdir(g_db_files_dirname.c_str(), 0700);
  }
  else
  {
    std::cerr << "DB directory already exists. Perhaps you want to start the machine in recover mode? Fatal error." << std::endl;
    exit(1); 
  }

  // cluster member list must exist <---------need boost
  struct stat stat_cluster_member_list;
  if (lstat(g_cluster_member_list_filename.c_str(), &stat_cluster_member_list) == -1) 
  {
    std::cerr << "Could not stat cluster member list file. Fatal error." << std::endl;
    exit(1); 
  }

  g_current_node_state->loadClusterMemberList(g_cluster_member_list_filename);

  // the intial partition map must exist <---------need boost
  struct stat stat_partition_table_file;
  if (lstat(g_cached_partition_map_filename.c_str(), &stat_partition_table_file) == -1) 
  {
    std::cerr << "Could not stat partition-node map file. Fatal error." << std::endl;
    exit(1); 
  }

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

  g_current_node_state->state = NodeState::STABLE;

  // save the partition state
  g_current_node_state->savePartitionState(g_owned_partition_state_filename);
  g_current_node_state->saveNodeState(g_current_node_state_filename);
}

// Citation: modified argument parsing function adapted from word2vec by T. Mikolov
static int ArgPos(const char *str, int argc, const char **argv, bool has_additional) 
{
  int i;
  for (i = 1; i < argc; i++) 
  {
    if (strcmp(str, argv[i]) == 0) 
    {
      if (has_additional && i == argc - 1) 
      {
        printf("Argument missing for %s\n", str);
        exit(1);
      }
      return i;
    }
  }
  return -1;
}

int main(int argc, const char *argv[])
{
  if (argc == 1) // print usage instructions
  {
    std::cout << "Usage: \n"
      << "\t--datadir <dirname> REQUIRED [place to store db shards]\n"
      << "\t--nodestate <filename> REQUIRED [current node state]\n"
      << "\t--clustermembers <filename> REQUIRED [list of cluster members]\n"
      << "\t--ownershipmap <filename> REQUIRED [list of partitions owned by current node]\n"
      << "\t--partitionmap <filename> REQUIRED [mapping of partitions to nodes]\n"
      << "\t--join OPTIONAL\n"
      << "\t--recover OPTIONAL\n"
      << "\t--debug OPTIONAL [start sending fake data to other cluster members]\n"
//      << "\t--name OPTIONAL [give the node a custom name that can be reached, IP address]"
      << std::endl;
      return 0;
  }

  int i;
  bool join = false, recover = false, debug = false;
  if ((i = ArgPos("--datadir", argc, argv, true)) > 0) 
    g_db_files_dirname = std::string(argv[i+1]);
  if ((i = ArgPos("--nodestate", argc, argv, true)) > 0) 
    g_current_node_state_filename = std::string(argv[i+1]);
  if ((i = ArgPos("--clustermembers", argc, argv, true)) > 0) 
    g_cluster_member_list_filename = std::string(argv[i+1]);
  if ((i = ArgPos("--ownershipmap", argc, argv, true)) > 0)
    g_owned_partition_state_filename = std::string(argv[i+1]);
  if ((i = ArgPos("--partitionmap", argc, argv, true)) > 0)
    g_cached_partition_map_filename = std::string(argv[i+1]);
  if ((i = ArgPos("--join", argc, argv, false)) > 0)
    join = true;
  if ((i = ArgPos("--recover", argc, argv, false)) > 0)
    recover = true;
  if ((i = ArgPos("--debug", argc, argv, false)) > 0)
    debug = true;

  std::cout << "DB directory : " << g_db_files_dirname << std::endl;
  std::cout << "Node state file : " << g_current_node_state_filename << std::endl;
  std::cout << "Cluster members file : " << g_cluster_member_list_filename << std::endl;
  std::cout << "Ownership map file : " << g_owned_partition_state_filename << std::endl;
  std::cout << "Partition-node map file : " << g_cached_partition_map_filename << std::endl;

  if (join && recover)
  {
    perror("cannot join and recover\n");
    exit(0);
  }
  
  if (g_db_files_dirname == "")
  {
    perror("must specify a directory for database files\n");
    exit(0);
  }
  if (g_current_node_state_filename == "")
  {
    perror("must specify a filename for persisting node state\n");
    exit(0);
  }
  if (g_cluster_member_list_filename == "")
  {
    perror("must specify a file for list of cluster members\n");
    exit(0);
  }
  if (g_owned_partition_state_filename == "")
  {
    perror("must specify a file for owned partition state\n");
    exit(0);
  }
  if (g_cached_partition_map_filename == "")
  {
    perror("must specify a file for partition map\n");
    exit(0);
  }

  edisense_comms::Member member;
  Server server;
  member.start(&server);

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

//  std::thread rebalance_thread(LoadBalanceDaemon, &member, 60 * 5); // 5 minutes
  std::thread gc_thread(GarbageCollectDaemon, 60 * 60 * 12); // 12 hrs
  
  if (debug) // simulate data
  {
    std::thread simulate_put_thread(SimulatePutDaemon, &member,1, 42);
    simulate_put_thread.detach();
  }

  gc_thread.join();
//  rebalance_thread.join();

  member.stop();
}