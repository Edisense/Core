
#include <list>
#include <ctime>
#include <future>
#include <cassert>
#include <thread>
#include <chrono>
#include <iostream>
#include <unistd.h>
#include <mutex>

#include "global.h"
#include "state.h"

#include "util/hash.h"
#include "server/server_internal.h"
#include "partition/partition_table.h"


#include <edisense_comms.h>
#include <member.h> 

#include "edisense_types.h"

#include "ble_client_internal.h"

static const std::chrono::milliseconds kPutRequestTimeOut(60000);

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
			std::cout << partition_owners[i] << std::endl;
			assert(g_current_node_state->cluster_members.find(partition_owners[i]) 
				!= g_current_node_state->cluster_members.end());
			std::string hostname = g_current_node_state->cluster_members[partition_owners[i]];
			partition_owners_hostnames.push_back(hostname);
		}
		else // current node owns this partition too
		{
			std::cout << "Put: Hey! I own this partition too! " << target_partition << std::endl;
			g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 2
			if (g_current_node_state->partitions_owned_map[target_partition].db)
			{
				success = g_current_node_state->partitions_owned_map[target_partition]
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
	g_cached_partition_table->lock.releaseRDLock(); // 1

	if (!partition_owners_hostnames.empty())
	{
		unsigned char *charBuf = (unsigned char*)data;
		blob dataBlob(charBuf, charBuf + data_len);

		std::future<std::list<std::pair<std::string, PutResult>>> future_result 
			= member->put(g_current_node_id, tid, partition_owners_hostnames, device_id, timestamp, expiration, dataBlob);

		// wait on the future
		if (future_result.wait_for(kPutRequestTimeOut) != std::future_status::ready)
		{
			return false;
		}
		std::list<std::pair<std::string, PutResult>> results = future_result.get();

		// check results of put
		success &= results.size() == partition_owners_hostnames.size();
		for (auto &kv: results)
		{
			success &= (kv.second.status == SUCCESS);
			if (kv.second.status == DATA_NOT_OWNED)
			{
				std::cout << "data not owned by " << kv.first << std::endl;
			}
			if (kv.second.status == DATA_MOVED) // update location in partition table
			{
				g_cached_partition_table->lock.acquireWRLock(); // 4
				node_t new_owner = hostToNodeId(kv.first);
				std::cout << "new owner " << kv.first << std::endl;
				assert(g_cached_partition_table->updatePartitionOwner(new_owner, kv.second.moved_to, target_partition));
				g_cached_partition_table->lock.releaseWRLock(); // 4
			} 
		}
	}

	return success;
}

typedef struct FailedPut 
{
	device_t device_id;
	time_t timestamp;
	time_t expiration;
	char buf[kMaxDataLen];
	uint8_t buflen;
} FailedPut;

static std::mutex async_put_queue_lock;
static std::list<FailedPut> async_put_queue;

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
		}
		else
		{
			std::cout << "Put failed: " << device_id << " (device) " << timestamp 
				<< " (timestamp) " << expiration << " (expiry) " << counter << " (value)"
				<< " [moving to async]" << std::endl;
			FailedPut fp;
			fp.device_id = device_id;
			fp.timestamp = timestamp;
			fp.expiration = expiration;
			*(fp.buf) = counter;
			fp.buflen = sizeof(unsigned long long);

			async_put_queue_lock.lock();
			async_put_queue.push_back(fp);
			async_put_queue_lock.unlock();
		}

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
}

void RetryPutDaemon(edisense_comms::Member *member, unsigned int freq)
{
	while (true)
	{
		sleep(freq);
		FailedPut fp; 
		async_put_queue_lock.lock();
		if (async_put_queue.empty())
		{
			async_put_queue_lock.unlock();
			continue;
		}
		else
		{	
			fp = async_put_queue.front();
			async_put_queue.pop_front();
			async_put_queue_lock.unlock();
		}

		if (Put(member, fp.device_id, fp.timestamp, fp.expiration, fp.buf, fp.buflen))
		{
			std::cout << "Async put successful: " << fp.device_id << " (device) " << fp.timestamp 
				<< " (timestamp) " << fp.expiration << " (expiry) " 
				<< std::endl;
		}
		else
		{
			std::cout << "Asyc put failed: " << fp.device_id << " (device) " << fp.timestamp 
				<< " (timestamp) " << fp.expiration << " (expiry) "
				<< " [appending to list]" << std::endl;
			async_put_queue_lock.lock();
			async_put_queue.push_back(fp);
			async_put_queue_lock.unlock();
		}
	}
}