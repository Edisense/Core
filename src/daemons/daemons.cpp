
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <iostream>
#include <future>
#include <cstdlib>
#include <iterator>

#include "include/global.h"

#include "src/util/utilization.h"
#include "src/ble/ble_client_internal.h"

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
		cout << "waking up to do load balancing" << endl;
		float current_utilization = ComputeNodeUtilization() / (float) g_local_disk_limit_in_bytes;
		if (current_utilization < kLoadBalanceThreshold)
		{
			cout << "not enough load to balance, going back to sleep" << endl;
			continue;

		}

		g_current_node_state.partition_map_lock.acquireWRLock();

		int random_elem = rand() % partition_map.size();
		std::iterator it = partition_map.begin();
		std::advance(it, random_elem);
		partition_t victim = it->first;
		PartitionMetadata pm = it->second;
		if (pm.state != stable)
		{
			g_current_node_state.partition_map_lock.releaseWRLock();
			continue;
		}

		// finish implementing eviction

		g_current_node_state.partition_map_lock.releaseWRLock();
	}
}

static const int kSecondsInDay = 60 * 60 * 24;
static const time_t kMinimumGCDelay = 60 * 60; // 1 hr

void GarbageCollectDaemon(unsigned int freq)
{
	assert (freq != 0); // this would be really dumb
	if (freq < kSecondsInDay / 2)
	{
		cout << "warning: setting a gc frequency too low can diminish performance" << endl;
	} 
	while (true)
	{
		sleep(freq);
		cout << "waking up to do garbage collection" << endl;
		time_t current_time;
		time(&current_time);
		assert (current_time > kMinimumGCDelay);

		time_t gc_before_time -= kMinimumGCDelay;

		g_current_node_state.partition_map_lock.acquireRDLock();
		for (auto &kv : g_current_node_state.partition_map)
		{
			PartitionMetadata pm = kv.second;
			pm.db->remove(gc_before_time);
		}
		g_current_node_state.partition_map_lock.releaseRDLock();
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
