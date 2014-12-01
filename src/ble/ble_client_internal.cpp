
#include <list>
#include <future>

#include "server/server_internal.h"

#include "ble_client_internal.h"

static const int kPutRequestTimeOut = 5;

bool Put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	int num_replicas = g_cached_partition_table.getNumReplicas();
	int target_partition = g_cached_partition_table.getPartition(hash_integer(device_id));

	transaction_t tid = g_cached_partition_table.getTransactionID();

	g_cached_partition_table.lock.acquireRDLock(); // 1
	partition_t *partition_owners g_cached_partition_table.getPartitionOwners(target_partition);
	
	bool success;
	std::list<std::string> partition_owners_hostnames;
	for (int i = 0; i < num_replicas; i++) 
	{
		if (partition_owners[i] != g_current_node_id)
		{
			partition_owners_hostnames.push_back(g_current_node_state.partition_map[partition_owners[i]]);
		}
		else // current node owns this partition too
		{
			g_current_node_state.partition_map_lock.acquireRDLock(); // 2
			success = g_current_node_state.partition_map[g_current_node_id]
				.db->put(device_id, timestamp, expiration, data, data_len));
			g_current_node_state.partition_map_lock.releaseRDLock(); // 2
		}
	}

	std::future<std::list<PutResult>> *future_result = SendPutRequest(tid, partition_owners, device_id, 
		timestamp, expiration, data, data_len);
	g_cached_partition_table.lock.releaseRDLock(); // 1

	// wait on the future
	if (future_result->wait_for(kPutRequestTimeOut) != future_status::ready)
	{
		FreeTransaction(tid); // timed out with no response
		return false;
	}
	result = future_result->get();

	// check results of put
	success &= results.size() == num_replicas;
	for (PutResult pr : results)
	{
		success &= pr.success;
		assert(pr.error != DATA_NOT_OWNED); // this would mean we are fundamentally inconsistent
		if (pr.error == DATA_MOVED) // update location in partition table
		{
			g_cached_partition_table.lock.acquireWRLock(); // 3
			assert(updatePartitionOwner(node_t old_owner, node_t new_owner, partition_t partition_no));
			g_cached_partition_table.lock.releaseWRLock(); // 3
		} 
	}

	FreeTransaction(tid); // free the future 
	return success;
}