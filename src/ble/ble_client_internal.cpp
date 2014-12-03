
#include <list>
#include <future>
#include <cassert>
#include <chrono>

#include "global.h"
#include "state.h"

#include "util/hash.h"
#include "server/server_internal.h"
#include "partition/partition_table.h"

//#include "send_interface.h"

#include "ble_client_internal.h"

static const std::chrono::milliseconds kPutRequestTimeOut(5000);

bool Put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	return false;
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
			if (g_current_node_state->partition_map[g_current_node_id].db)
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

	std::future<std::list<PutResult> > future_result = SendPutRequest(tid, partition_owners, device_id, 
		timestamp, expiration, data, data_len);
	g_cached_partition_table->lock.releaseRDLock(); // 1

	// wait on the future
	if (future_result.wait_for(kPutRequestTimeOut) != std::future_status::ready)
	{
		FreeTransaction(tid); // timed out with no response
		return false;
	}
	std::list<std::pair<node_t, PutResult> > results = future_result.get();

	// check results of put
	success &= results.size() == num_replicas;
	for (auto &kv: results)
	{
		success &= kv.second.success;
		assert(kv.second.error != DATA_NOT_OWNED); // this would mean we are fundamentally inconsistent
		if (kv.second.error == DATA_MOVED) // update location in partition table
		{
			g_cached_partition_table->lock.acquireWRLock(); // 4
			assert(g_cached_partition_table->updatePartitionOwner(kv.first, kv.second.moved_to, target_partition));
			g_cached_partition_table->lock.releaseWRLock(); // 4
		} 
	}

	FreeTransaction(tid); // free the future 
	return success;
}