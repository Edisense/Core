
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <iostream>
#include <future>
#include <cstdlib>
#include <iterator>

#include "global.h"

#include "util/utilization.h"
#include "util/socket-util.h"

#include "ble/ble_client_internal.h"

#include "db_file_transfer.h"

#include "daemons.h"

// minimum load before load balancing
static const float kLoadBalanceThreshold = 0.8;

void LoadBalanceDaemon(unsigned int freq)
{
	assert (freq != 0);
	while (true)
	{
		sleep(freq);
		std::cout << "waking up to do load balancing" << std::endl;
		float current_utilization = ComputeNodeUtilization() / (float) g_local_disk_limit_in_bytes;
		if (current_utilization < kLoadBalanceThreshold)
		{
			std::cout << "not enough load to balance, going back to sleep" << std::endl;
			continue;

		}

		// choose partition to try to donate
		g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 1
		int random_elem = rand() % g_current_node_state->partitions_owned_map.size();
		auto it = g_current_node_state->partitions_owned_map.begin();
		std::advance(it, random_elem);
		partition_t victim_partition = it->first; // select a partition to try to move away
		PartitionMetadata pm = it->second;
		if (pm.state != PartitionState::STABLE) // if the partition is not stable, fail the donate
		{
			g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1 
			continue;
		}
		else
		{
			g_current_node_state->partitions_owned_map_lock.releaseRDLock() // 1
		}

		// choose node to receive data
		g_current_node_state->cluster_members_lock.acquireRDLock(); // 2
		auto it2 = g_current_node_state->cluster_members.begin();
		std::advance(it2, random_elem);
		node_t node_id = it2->first;
		std::string node_hostname = it2->second;
		g_current_node_state->cluster_members_lock.releaseRDLock(); // 2

		// send commit move request

		// LOG presend

		transaction_t commit_recv_tid = g_current_node_state->getTransactionID();
		std::future<bool> future_commit_recv 
			= SendCommitReceiveRequest(commit_recv_tid, node_hostname, victim_partition);
		if (future_commit_recv.wait() != std::future_status::ready) // must get a reply back to continue
		{
			FreeTransaction(commit_recv_tid);
			continue;
		}

		if (!future_commit_recv.get()) // abort transfer
		{
			FreeTransaction(commit_recv_tid);
			// LOG failure
			continue;
		}
		FreeTransaction(commit_recv_tid);
		
		// LOG commit -- point of no return

		g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 3
		g_current_node_state->partitions_owned_map[victim_partition].state = PartitionState::DONATING;
		g_current_node_state->partitions_owned_map[victim_partition].other_node = node_id;
		g_current_node_state->savePartitionState(g_owned_partition_state_filename); // persist to disk
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 3

		g_cached_partition_table->lock.acquireWRLock();
		g_cached_partition_table->updatePartitionOwner(g_current_node_id, node_id, victim_partition);
		g_cached_partition_table->lock.releaseWRLock();

		// keep trying to send db file
		std::thread send_db_thread([&]()
		{
			while(!SendDBFile(node_hostname, GetPartitionDBFilename(victim_partition), victim_partition));
		});

		// tell other nodes that the 
		std::set<std::string> hostnames_not_heard_back;
		g_current_node_state->cluster_members_lock.acquireRDLock(); // 4
		for (auto &kv : g_current_node_state->cluster_members)
		{
			if (kv.first != node_id && kv.first != g_current_node_id)
				other_nodes.insert(kv.second);
		}
		g_current_node_state->cluster_members_lock.releaseRDLock(); // 4

		transaction_t ack_update_tid = g_current_node_state->getTransactionID();
		std::future<std::set<string>> future_ackers = SendUpdatePartitionOwner(ack_update_tid, 
		hostnames_not_heard_back, node_id, victim_partition);
		future_ackers.wait();
		send_db_thread.join();

		// LOG all acked and transferred

		// No longer own the range
		g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 3
		g_current_node_state->partitions_owned_map.erase(victim_partition);
		g_current_node_state->savePartitionState(g_owned_partition_state_filename); // persist to disk
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 3

		transaction_t finalize_transaction_tid;
		std::future<bool> SendCommitAsStableRequest(finalize_transaction_tid, node_hostname, victim_partition)

		// LOG complete
	}
}

static const int kSecondsInDay = 60 * 60 * 24;
static const time_t kMinimumGCDelay = 60 * 60; // 1 hr

void GarbageCollectDaemon(unsigned int freq)
{
	assert (freq != 0); // this would be really dumb
	if (freq < kSecondsInDay / 2)
	{
		std::cout << "warning: setting a gc frequency too low can diminish performance" << std::endl;
	} 
	while (true)
	{
		sleep(freq);
		std::cout << "waking up to do garbage collection" << std::endl;
		time_t current_time;
		time(&current_time);
		assert (current_time > kMinimumGCDelay);

		time_t gc_before_time = current_time - kMinimumGCDelay;

		g_current_node_state->partitions_owned_map_lock.acquireRDLock();
		for (auto &kv : g_current_node_state->partitions_owned_map)
		{
			PartitionMetadata pm = kv.second;
			pm.db->remove(gc_before_time);
		}
		g_current_node_state->partitions_owned_map_lock.releaseRDLock();
	}
}

void RetryPutDaemon(unsigned int freq)
{
	// TODO not implemented
}

static const int kDBTransferServerBacklog = 5;
static const unsigned short kDBTransferServerPortNo = 4000;
void DBTransferServerDaemon()
{
	int server_fd = createServerSocket(kDBTransferServerPortNo, kDBTransferServerBacklog);
  	if (server_fd == kServerSocketFailure)
  	{
    	throw "Unable to start db transfer server.";
  	}

  	struct sockaddr_in cli_addr;
  	socklen_t cli_len = sizeof(cli_addr);
	while (true)
	{
	  	int client_fd = accept(server_fd, (struct sockaddr *) &cli_addr, &cli_len);
	  	if (client_fd < 0)
	  	{
	  		perror("Error on accept");
	  		close(client_fd);
	  		continue;
	  	}

	  	ReceiveDBFile(client_fd);
	}
}
