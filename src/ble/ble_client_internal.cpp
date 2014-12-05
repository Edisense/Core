
#include <list>
#include <ctime>
#include <future>
#include <cassert>
#include <thread>
#include <chrono>
#include <iostream>
#include <unistd.h>

#include "global.h"
#include "state.h"

#include "util/hash.h"
#include "server/server_internal.h"
#include "partition/partition_table.h"


#include <edisense_comms.h>
#include <member.h> 

#include "edisense_types.h"

#include "ble_client_internal.h"

static const std::chrono::milliseconds kPutRequestTimeOut(5000);

bool Put(edisense_comms::Member *member, device_t device_id, time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	int num_replicas = g_cached_partition_table->getNumReplicas();
	int target_partition = g_cached_partition_table->getPartition(hash_integer(device_id));

	transaction_t tid = g_current_node_state->getTransactionID();

	g_cached_partition_table->lock.acquireRDLock(); // 1
	g_current_node_state->cluster_members_lock.acquireRDLock(); // 3
	
	node_t *partition_owners = g_cached_partition_table->getPartitionOwners(target_partition);
	bool success;
	std::list<std::string> partition_owners_hostnames;
	for (int i = 0; i < num_replicas; i++) 
	{
		if (partition_owners[i] != g_current_node_id)
		{
			partition_owners_hostnames.push_back(g_current_node_state->cluster_members[partition_owners[i]]);
		}
		else // current node owns this partition too
		{
			g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 2
			if (g_current_node_state->partitions_owned_map[g_current_node_id].db)
			{
				success = g_current_node_state->partitions_owned_map[g_current_node_id]
					.db->put(device_id, timestamp, expiration, data, data_len);
			}
			else
			{
				success = false;
			}
			g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 2
		}
	}
	g_current_node_state->cluster_members_lock.releaseRDLock(); // 3

	std::future<std::list<std::pair<std::string, PutResult>>> future_result 
		= member->put(tid, partition_owners, device_id, timestamp, expiration, data, data_len);
	g_cached_partition_table->lock.releaseRDLock(); // 1

	// wait on the future
	if (future_result.wait_for(kPutRequestTimeOut) != std::future_status::ready)
	{
		return false;
	}
	std::list<std::pair<std::string, PutResult>> results = future_result.get();

	// check results of put
	success &= results.size() == num_replicas;
	for (auto &kv: results)
	{
		success &= (kv.second.status == SUCCESS);
		assert(kv.second.status != DATA_NOT_OWNED); // this would mean we are fundamentally inconsistent
		if (kv.second.status == DATA_MOVED) // update location in partition table
		{
			g_cached_partition_table->lock.acquireWRLock(); // 4
			assert(g_cached_partition_table->updatePartitionOwner(hostToNodeId(kv.first), kv.second.moved_to, target_partition));
			g_cached_partition_table->lock.releaseWRLock(); // 4
		} 
	}

	return success;
}

static const int kSecondsInDay = 60 * 60 * 24;
void SimulatePutDaemon(edisense_comms::Member *member, unsigned int freq, device_t device_id)
{
	assert(freq != 0);
	unsigned long long counter = 0; // we will use this as our data
	time_t timestamp;
	time(&timestamp);
	time_t expiration = timestamp + kSecondsInDay; // 1 day later 
	while (true)
	{
		sleep(freq);
		if (Put(member, device_id, timestamp, expiration, &counter, sizeof(unsigned long long)))
		{
			std::cout << "Put successful: " << device_id << " (device) " << timestamp 
				<< " (timestamp) " << expiration << " (expiry) " << counter << " (value)"
				<< std::endl;
			counter++;
			time_t new_timestamp;
			time(&new_timestamp);
			do 
			{
				time(&new_timestamp);
			}
			while (new_timestamp <= timestamp); // make sure timestamps do not conflict
			timestamp = new_timestamp;
			expiration = timestamp + kSecondsInDay;
		}
		else
		{
			std::cout << "Put failed: " << device_id << " (device) " << timestamp 
				<< " (timestamp) " << expiration << " (expiry) " << counter << " (value)"
				<< std::endl;
		}
	}
}