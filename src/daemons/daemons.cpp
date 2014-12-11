
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
#include <string>
#include <set>
#include <list>
#include <thread>
#include <edisense_comms.h>
#include <member.h>

#include "global.h"

#include "util/utilization.h"
#include "util/socket-util.h"

#include "ble/ble_client_internal.h"

#include "daemons/db_file_transfer.h"
#include "daemons/rebalance_log.h"

#include "daemons.h"

// minimum load before load balancing
static const float kLoadBalanceThreshold = 0.85;

static const unsigned short kDbFileTransferPort = 5000;

static void transferDBFileInBackground(std::string hostname, partition_t partition)
{
	int sockfd = createClientSocket(hostname, kDbFileTransferPort);
	std::string filename = GetPartitionDBFilename(partition);
	while(!SendDBFile(sockfd, hostname, filename, partition));	
	close(sockfd);
}

#define REMOTE_TIMEOUT 2000

static bool PickVictimPartition(partition_t *ret)
{
	// choose partition to try to donate
	g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1
	int random_elem = rand() % g_current_node_state->partitions_owned_map.size();
	auto it = g_current_node_state->partitions_owned_map.begin();
	std::advance(it, random_elem);
	*ret = it->first; // select a partition to try to move away
	PartitionMetadata *pm = &it->second;
	if (pm->state != PartitionState::STABLE || pm->pinned) // if the partition is not stable, fail the donate
	{
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1 
		return false;
	}
	else
	{
		pm->pinned = true;
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1
		return true;
	}
}

static void UnpickVictimPartition(partition_t partition_id)
{
	g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1
	if (g_current_node_state->partitions_owned_map.find(partition_id) 
		!= g_current_node_state->partitions_owned_map.end())
	{
		g_current_node_state->partitions_owned_map[partition_id].pinned = false;
	}
	g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1
}

static bool PickRecipient(partition_t partition_id, edisense_comms::Member *member, node_t *ret, std::string &hostname)
{
	int random_elem;
	g_current_node_state->cluster_members_lock.acquireRDLock(); // 2
	random_elem = rand() % g_current_node_state->cluster_members.size();
	auto it1 = g_current_node_state->cluster_members.begin();
	std::advance(it1, random_elem);
	node_t node_id_1 = it1->first;
	std::string node_hostname_1 = it1->second;

	random_elem = rand() % g_current_node_state->cluster_members.size();
	auto it2 = g_current_node_state->cluster_members.begin();
	std::advance(it2, random_elem);
	node_t node_id_2 = it2->first;
	std::string node_hostname_2 = it2->second;
	g_current_node_state->cluster_members_lock.releaseRDLock(); // 2

	transaction_t tid1 = g_current_node_state->getTransactionID();
	std::future<CanReceiveResult> future_response_1 = member->canReceiveRequest(g_current_node_id, tid1, node_hostname_1, partition_id);

	transaction_t tid2 = g_current_node_state->getTransactionID();
	std::future<CanReceiveResult> future_response_2 = member->canReceiveRequest(g_current_node_id, tid2, node_hostname_2, partition_id);

	std::future_status status = future_response_1.wait_for(std::chrono::milliseconds(REMOTE_TIMEOUT));
	if (status != std::future_status::ready) // must get a reply back to continue
	{
		return false;
	}

	status = future_response_2.wait_for(std::chrono::milliseconds(REMOTE_TIMEOUT));
	if (status != std::future_status::ready) // must get a reply back to continue
	{
		return false;
	}

	CanReceiveResult can_recv_result_1 = future_response_1.get();
	CanReceiveResult can_recv_result_2 = future_response_2.get();

	bool success = true;
	if (can_recv_result_1.can_recv && can_recv_result_2.can_recv)
	{
		*ret = (can_recv_result_1.free > can_recv_result_2.free) ? node_id_1 : node_id_2;
		hostname = (can_recv_result_1.free > can_recv_result_2.free) ? node_hostname_1 : node_hostname_2;
	}
	else if (can_recv_result_1.can_recv)
	{
		*ret = node_id_1; 
		hostname = node_hostname_1;
	}
	else if (can_recv_result_2.can_recv)
	{
		*ret = node_id_2;
		hostname = node_hostname_2;
	}
	else
	{
		success = false;
	}
	return success;
}	

static bool CommitReceiveToRecipient(partition_t partition_id, edisense_comms::Member *member, std::string &hostname)
{
	transaction_t commit_recv_tid = g_current_node_state->getTransactionID();
	std::future<bool> future_commit_recv = 
		member->commitReceiveRequest(g_current_node_id, commit_recv_tid, hostname, partition_id);
	std::future_status status = future_commit_recv.wait_for(std::chrono::milliseconds(REMOTE_TIMEOUT));
	if (status != std::future_status::ready) // must get a reply back to continue
	{
		return false;
	}
	return future_commit_recv.get();
}

// TODO Extract this into functions - I know opinions differ on approriate length, but 100 is too many ;) - JR
void LoadBalanceDaemon(edisense_comms::Member *member, unsigned int freq, std::string logfile)
{
	RebalanceLog rb_log(logfile);

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
		std::cout << "picking partition to donate" << std::endl;
		partition_t victim_partition;
		if (!PickVictimPartition(&victim_partition)) continue;

		std::cout << "picking recipient" << std::endl;
		node_t receiving_node_id;
		std::string receiving_node_hostname;
		if (!PickRecipient(victim_partition, member, &receiving_node_id, receiving_node_hostname))
		{
			std::cout << "abort donation, failed to pick recipient" << std::endl;
			UnpickVictimPartition(victim_partition);
			continue;
		}

		// send commit move request

		rb_log.logPresend(victim_partition, receiving_node_id);

		std::cout << "committing to recipient" << std::endl;
		if (!CommitReceiveToRecipient(victim_partition, member, receiving_node_hostname))
		{
			// LOG failure
			std::cout << "abort donation, recipient replied no or failed" << std::endl;
			rb_log.logAbort(victim_partition, receiving_node_id);
			UnpickVictimPartition(victim_partition);
			continue;
		}
		
		// LOG commit -- point of no return
		rb_log.logCommit(victim_partition, receiving_node_id);

		std::cout << "committing donate" << std::endl;
		g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 3
		g_current_node_state->partitions_owned_map[victim_partition].state = PartitionState::DONATING;
		g_current_node_state->partitions_owned_map[victim_partition].other_node = receiving_node_id;
		delete g_current_node_state->partitions_owned_map[victim_partition].db; // close reference to the db
		g_current_node_state->savePartitionState(g_owned_partition_state_filename); // persist to disk
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 3

		g_cached_partition_table->lock.acquireWRLock();
		g_cached_partition_table->updatePartitionOwner(g_current_node_id, receiving_node_id, victim_partition);
		g_cached_partition_table->lock.releaseWRLock();

		std::string partition_filename = GetPartitionDBFilename(victim_partition);

		// keep trying to send db file
		std::thread send_db_thread(transferDBFileInBackground, receiving_node_hostname, victim_partition);

		// tell other nodes that the partition is being donated
		std::list<std::string> hostnames_not_heard_back;
		g_current_node_state->cluster_members_lock.acquireRDLock(); // 4
		for (auto &kv : g_current_node_state->cluster_members)
		{
			if (kv.first != receiving_node_id && kv.first != g_current_node_id)
				hostnames_not_heard_back.push_back(kv.second);
		}
		g_current_node_state->cluster_members_lock.releaseRDLock(); // 4

		transaction_t ack_update_tid = g_current_node_state->getTransactionID();

		while (true)
		{
			std::future<std::list<std::string>> future_ackers 
				= member->updatePartitionOwner(g_current_node_id, ack_update_tid, hostnames_not_heard_back, receiving_node_id, victim_partition);
			future_ackers.wait();
			std::list<std::string> hostnames_not_heard_back = future_ackers.get();
			if (hostnames_not_heard_back.empty()) break;
		}

		send_db_thread.join();

		// LOG all acked and transferred
		rb_log.logAckedAndTransferred(victim_partition);

		std::cout << "done donating, sending complete message" << std::endl;

		// No longer own the range
		g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 3
		g_current_node_state->partitions_owned_map.erase(victim_partition);
		g_current_node_state->savePartitionState(g_owned_partition_state_filename); // persist to disk
		g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 3

		transaction_t finalize_transaction_tid;

		while (true)
		{
			std::future<bool> future_finalize = member->commitAsStableRequest(g_current_node_id, finalize_transaction_tid, receiving_node_hostname, victim_partition);
			future_finalize.wait();
			if (future_finalize.get()) break;
		}

		// LOG complete
		rb_log.logComplete(victim_partition);

		std::cout << "donation finished, removing db shard" << std::endl;

		remove(partition_filename.c_str());
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
    	perror("Unable to start db transfer server");
    	exit(1);
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
