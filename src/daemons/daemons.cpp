
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
#include <vector>

#include <edisense_comms.h>
#include <member.h>

#include "global.h"

#include "util/utilization.h"
#include "util/socket-util.h"

#include "ble/ble_client_internal.h"

#include "daemons/db_file_transfer.h"
#include "daemons/rebalance_log.h"

#include "daemons.h"

static const int kRemoteTimeOut = 10000;

static const bool kForceDonate = false;

// minimum load before load balancing
static const float kLoadBalanceThreshold = 0.85;

static const unsigned short kDbFileTransferPort = 5000;

static void transferDBFileInBackground(std::string hostname, partition_t partition)
{
	int sockfd = createClientSocket(hostname, kDbFileTransferPort);
	std::string filename = GetPartitionDBFilename(partition);
	std::cout << "try send database file" << std::endl;
	while(!SendDBFile(sockfd, hostname, filename, partition));	
	std::cout << "done sending database file" << std::endl;
	close(sockfd);
}

static bool pickVictimPartition(partition_t *ret)
{
	// choose partition to try to donate
	g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1

	int partitions_owned = g_current_node_state->partitions_owned_map.size();
	if (partitions_owned == 0)
	{
		g_current_node_state->partitions_owned_map_lock.releaseWRLock();
		return false;
	}

	int random_elem = rand() % partitions_owned;
	auto it = g_current_node_state->partitions_owned_map.begin();
	std::advance(it, random_elem);
	*ret = it->first; // select a partition to try to move away

	std::cout << *ret << std::endl;  

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

static void unpickVictimPartition(partition_t partition_id)
{
	g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1
	if (g_current_node_state->partitions_owned_map.find(partition_id) 
		!= g_current_node_state->partitions_owned_map.end())
	{
		g_current_node_state->partitions_owned_map[partition_id].pinned = false;
	}
	g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1
}

static bool pickRecipient(partition_t partition_id, edisense_comms::Member *member, node_t *ret, std::string &hostname)
{
	int random_elem;
	g_current_node_state->cluster_members_lock.acquireRDLock(); // 2
	node_t node_id_1;
	std::string node_hostname_1;
	do 
	{
		random_elem = rand() % g_current_node_state->cluster_members.size();
		auto it = g_current_node_state->cluster_members.begin();
		std::advance(it, random_elem);
		node_id_1 = it->first;
	 	node_hostname_1 = it->second;
	}
	while (node_id_1 == g_current_node_id);

	node_t node_id_2;
	std::string node_hostname_2;
	do 
	{
		random_elem = rand() % g_current_node_state->cluster_members.size();
		auto it = g_current_node_state->cluster_members.begin();
		std::advance(it, random_elem);
		node_id_2 = it->first;
	 	node_hostname_2 = it->second;
	}
	while (node_id_2 == g_current_node_id);

	g_current_node_state->cluster_members_lock.releaseRDLock(); // 2

	transaction_t tid1 = g_current_node_state->getTransactionID();
	std::future<CanReceiveResult> future_response_1 = member->canReceiveRequest(g_current_node_id, tid1, node_hostname_1, partition_id);

	transaction_t tid2 = g_current_node_state->getTransactionID();
	std::future<CanReceiveResult> future_response_2 = member->canReceiveRequest(g_current_node_id, tid2, node_hostname_2, partition_id);

	std::future_status status = future_response_1.wait_for(std::chrono::milliseconds(kRemoteTimeOut));
	if (status != std::future_status::ready) // must get a reply back to continue
	{
		std::cout << node_hostname_1 << "timed out" << std::endl;
		return false;
	}

	status = future_response_2.wait_for(std::chrono::milliseconds(kRemoteTimeOut));
	if (status != std::future_status::ready) // must get a reply back to continue
	{
		std::cout << node_hostname_2 << "timed out" << std::endl;
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
		std::cout << "donate rejected" << std::endl;
		success = false;
	}
	return success;
}	

static bool commitReceiveToRecipient(partition_t partition_id, edisense_comms::Member *member, std::string &hostname)
{
	transaction_t commit_recv_tid = g_current_node_state->getTransactionID();
	int backoff = 2;
	while (true)
	{
		std::future<CallStatusBool> future_commit_recv = 
			member->commitReceiveRequest(g_current_node_id, commit_recv_tid, hostname, partition_id);
		future_commit_recv.wait();
		CallStatusBool result = future_commit_recv.get();
		if (result == RET_TRUE)
			return true;
		else if (result == RET_FALSE)
			return false;
		sleep(backoff);
		backoff *= backoff;
	}
	return false; // unreachable
}

static void transferAndNotifyHelper(edisense_comms::Member *member, partition_t victim_partition, 
	node_t receiving_node_id, std::string &receiving_node_hostname, std::string partition_filename)
{
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

	int backoff = 2;
	while (true)
	{
		std::future<std::list<std::string>> future_ackers 
		= member->updatePartitionOwner(g_current_node_id, ack_update_tid, hostnames_not_heard_back, receiving_node_id, victim_partition);
		std::cout << "waiting for update partition owner acks" << std::endl;
		future_ackers.wait();
		std::cout << "received updating partition owner acks" << std::endl;
		std::list<std::string> hostnames_not_heard_back = future_ackers.get();
		if (hostnames_not_heard_back.empty()) break;
		else 
		{
			sleep(backoff);
			backoff *= backoff;
		}
	}

	send_db_thread.join();
}

static void completeDonateHelper(edisense_comms::Member *member, partition_t victim_partition, 
	std::string &receiving_node_hostname)
{
	std::cout << "done donating, sending complete message" << std::endl;

	// No longer own the range
	g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 3
	g_current_node_state->partitions_owned_map.erase(victim_partition);
	g_current_node_state->savePartitionState(g_owned_partition_state_filename); // persist to disk
	g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 3

	transaction_t finalize_transaction_tid = g_current_node_state->getTransactionID();

	int backoff = 2;
	while (true) // wait for arfirmative reply
	{
		std::future<bool> future_finalize = member->commitAsStableRequest(g_current_node_id, 
			finalize_transaction_tid, receiving_node_hostname, victim_partition);
		future_finalize.wait();
		if (future_finalize.get()) break;
		else 
		{
			sleep(backoff);
			backoff *= backoff;
		}
	}
}

static void loadBalanceAsync(edisense_comms::Member *member, RebalanceLog &rb_log)
{
	std::cout << "waking up to do load balancing" << std::endl;
	float current_utilization = ComputeNodeUtilization() / (float) g_local_disk_limit_in_bytes;
	if (current_utilization < kLoadBalanceThreshold)
	{
		std::cout << "not enough load to balance, going back to sleep" << std::endl;
		if (!kForceDonate) 
			return;
	}

	// choose partition to try to donate
	std::cout << "picking partition to donate" << std::endl;
	partition_t victim_partition;
	if (!pickVictimPartition(&victim_partition)) 
	{	
		std::cout << "failed to pick victim partition" << std::endl;
		return;
	}

	std::cout << "picking recipient" << std::endl;
	node_t receiving_node_id;
	std::string receiving_node_hostname;
	if (!pickRecipient(victim_partition, member, &receiving_node_id, receiving_node_hostname))
	{
		std::cout << "abort donation, failed to pick recipient" << std::endl;
		unpickVictimPartition(victim_partition);
		return;
	}

	// send commit move request

	rb_log.logPresend(victim_partition, receiving_node_id);

	std::cout << "committing to recipient" << std::endl;
	if (!commitReceiveToRecipient(victim_partition, member, receiving_node_hostname))
	{
		// LOG failure
		std::cout << "abort donation, recipient replied no or failed" << std::endl;
		rb_log.logAbort(victim_partition);
		unpickVictimPartition(victim_partition);
		return;
	}

	// LOG commit -- point of no return
	rb_log.logCommit(victim_partition, receiving_node_id);

	std::string partition_filename = GetPartitionDBFilename(victim_partition);
	transferAndNotifyHelper(member, victim_partition, receiving_node_id, 
		receiving_node_hostname, partition_filename);

	// LOG all acked and transferred
	rb_log.logAckedAndTransferred(victim_partition, receiving_node_id);

	completeDonateHelper(member, victim_partition, receiving_node_hostname);

	// LOG complete
	rb_log.logComplete(victim_partition);

	std::cout << "donation finished, removing db shard" << std::endl;

	remove(partition_filename.c_str());
}

// this is not so clean at the moment, should be merged into the above
static void loadBalanceRecoverAsync(edisense_comms::Member *member, RebalanceLog &rb_log, PendingDonate &pd, 
	 std::string receiving_node_hostname)
{
	if (pd.status == DS_PRESEND)
	{
		std::cout << "committing to recipient" << std::endl;
		if (!commitReceiveToRecipient(pd.partition_id, member, receiving_node_hostname))
		{
			// LOG failure
			std::cout << "abort donation, recipient replied no or failed" << std::endl;
			rb_log.logAbort(pd.partition_id);
			return;
		}
		// LOG commit -- point of no return
		rb_log.logCommit(pd.partition_id, pd.recipient);
	}
	
	std::string partition_filename = GetPartitionDBFilename(pd.partition_id);

	if (pd.status <= DS_COMMIT)
	{
		transferAndNotifyHelper(member, pd.partition_id, pd.recipient, 
			receiving_node_hostname, partition_filename);
		
		// LOG all acked and transferred
		rb_log.logAckedAndTransferred(pd.partition_id, pd.recipient);
	}

	if (pd.status <= DS_ACKED)
	{
		completeDonateHelper(member, pd.partition_id, receiving_node_hostname);
		// LOG complete
		rb_log.logComplete(pd.partition_id);
		std::cout << "donation finished, removing db shard" << std::endl;
		remove(partition_filename.c_str());
	}
}

static void recoverFailedDonateState(edisense_comms::Member *member, RebalanceLog &rb_log)
{
	std::list<PendingDonate> to_resume = rb_log.parseLog();
	std::cout << "resuming donations for recovery" << std::endl;
	std::vector<std::thread> workers;
	for (PendingDonate &pd : to_resume)
	{
		g_current_node_state->cluster_members_lock.acquireRDLock();
		std::string recipient_hostname = g_current_node_state->cluster_members[pd.recipient];
		g_current_node_state->cluster_members_lock.releaseRDLock();
		workers.push_back(std::thread(loadBalanceRecoverAsync, member, ref(rb_log), ref(pd), recipient_hostname));
	}
	for (std::thread &t : workers) t.join();
	std::cout << "done resuming donations" << std::endl;
}

void LoadBalanceDaemon(edisense_comms::Member *member, unsigned int freq, std::string logfile, bool recover)
{
	assert (freq != 0);
	RebalanceLog rb_log(logfile);
	if (recover)
	{
		recoverFailedDonateState(member, rb_log);
	}
	else
	{
		// if log already exists, such as when starting the program incorrectly
		// rename it
		std::string renamed = logfile + ".old";
		rename(logfile.c_str(), renamed.c_str());
	}
	
	while (true)
	{
		sleep(freq);
		std::thread t(loadBalanceAsync, member, ref(rb_log));
		t.detach();
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

static const int kDBTransferServerBacklog = 5;
void DBTransferServerDaemon()
{
	int server_fd = createServerSocket(kDbFileTransferPort, kDBTransferServerBacklog);
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
