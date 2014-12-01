
#include <list>

#include "server/server_internal.h"

#include "ble_client_internal.h"

static const int kPutRequestTimeOut = 5;

bool Put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	int num_replicas = g_cached_partition_table.getNumReplicas();
	int target_partition = g_cached_partition_table.getPartition(hash_integer(device_id));

	transaction_t tid = g_cached_partition_table.getTransactionID();

	g_cached_partition_table.lock.acquireRDLock();
	std::list<node_t> partition_owners g_cached_partition_table.getPartitionOwners(target_partition);
	SendPutRequest(tid, partition_owners, device_id, timestamp, expiration, data, data_len);
	g_cached_partition_table.lock.releaseRDLock();

	std::list<PutResult> results = WaitForPutResult(tid, kPutRequestTimeOut);

	bool success = true;
	success &= results.size() == num_replicas;
	for (PutResult pr : results)
	{
		success &= pr.success 
		if (pr.moved) 
		{
			g_cached_partition_table.lock.acquireWRLock();
			assert(updatePartitionOwner(node_t old_owner, node_t new_owner, partition_t partition_no));
			g_cached_partition_table.lock.releaseWRLock();
		}
	}

	return success;
}