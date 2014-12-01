
#include <cassert>

#include "include/state.h"
#include "include/global.h"
#include "src/partition/partition_db.h"
#include "src/util/hash.h"

#include "server_internal.h"

PutResult HandlePutRequest(MessageId mesg_id, device_t device_id, 
	time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	assert (g_current_node_id != mesg_id.node_id);

	PutResult ret;
	partition_t partition_to_put_into = g_cached_partition_table.getPartition(hash_integer(device_id));

	PartitionMetadata partition_state;
	g_current_node_state.partition_map_lock.acquireRDLock(); // 1

	try
	{
		partition_state = g_current_node_state.partition_map.at(partition_to_put_into);
	}
	catch (const std::out_of_range &e) // this range is not owned by current node!
	{
		cout << "received put on unowned range!" << endl;
		ret.success = false;
		ret.error = DATA_NOT_OWNED;
		g_current_node_state.partition_map_lock.releaseRDLock(); // 1
		return ret;
	}

	switch (partition_state.state)
	{
	case STABLE:
	case RECEIVED:
		PartitionDB *db = partition_state.db;
		ret.success = db->put(device_id, timestamp, expiration, data, data_len);
		if (!ret.success) ret.error = DB_ERROR;
		break;
	case RECEIVING: // not ready to handle put yet
		ret.success = false;
		ret.error = DATA_MOVING;
	case DONATING:
		ret.success = false;
		ret.error = DATA_MOVED;
		ret.moved_to = partition_state.other_node;
		break;
	}
	g_current_node_state.partition_map_lock.releaseRDLock(); // 1

	return ret;
}

static const float kMaximumUtilizationToAcceptDonation = 0.6;
static const size_t kMinBytesFreeToAcceptDonation = 512 * 1024 * 1024; // 512mb
CanReceiveResult HandleCanReceiveRequest(MessageId mesg_id, partition_t partition_id)
{
	assert (g_current_node_id != mesg_id.node_id);

	CanReceiveResult ret;
	size_t used = GetNodeUtilization()
	ret.free = g_local_disk_limit_in_bytes - used;
	ret.util = ret.free / (float) g_local_disk_limit_in_bytes;
	if (ret.util > kMaximumUtilizationToAcceptDonation || ret.free < kMinBytesFreeToAcceptDonation)
	{
		ret.can_recv = false;
		return ret;
	}

	// don't need the state lock since it is a hint
	if (g_current_node_state.state != NodeState::STABLE) // can only accept if stable
	{
		ret.can_recv = false;
		return ret;
	}
	
	g_current_node_state.partition_map_lock.acquireRDLock(); // 1
	if (g_current_node_state.partition_map.find(partition_id) 
		!= g_current_node_state.partition_map.end()) // check if storing partition already
	{
		ret.can_recv = false;
		g_current_node_state.partition_map_lock.releaseRDLock(); // 1
		return;
	}
	else
	{
		g_current_node_state.partition_map_lock.releaseRDLock(); // 1
	}

	ret.can_recv = true;
	return ret;
}

bool HandleCommitReceiveRequest(MessageId mesg_id, partition_t partition_id)
{
	assert (g_current_node_id != mesg_id.node_id);

	size_t used = GetNodeUtilization() // TODO what if we over commit?
	ret.free = g_local_disk_limit_in_bytes - used;
	ret.util = ret.free / (float) g_local_disk_limit_in_bytes;
	if (ret.util > kMaximumUtilizationToAcceptDonation || ret.free < kMinBytesFreeToAcceptDonation)
	{
		return false;
	}

	g_current_node_state.state_lock.acquireRDLock(); // 1
	if (g_current_node_state.state != NodeState::STABLE) // can only accept if stable
	{
		g_current_node_state.state_lock.releaseRDLock(); // 1
		return false;
	}
	
	g_current_node_state.partition_map_lock.acquireWRLock(); // 2
	if (g_current_node_state.partition_map.find(partition_id) 
		!= g_current_node_state.partition_map.end()) // check if storing partition already
	{
		PartitionMetadata pm = g_current_node_state.partition_map[partition_id];
		if (pm.state == PartitionState::PREPARING
			&& pm.other_node == mesg_id.node_id) // retransmission of comfirm receive
		{
			// reply true
		}
		else // already own or receiving the partition from another source
		{
			g_current_node_state.partition_map_lock.releaseWRLock(); // 2
			g_current_node_state.state_lock.releaseRDLock(); // 1
			return false;
		}
	}
	else // add to partition map
	{
		PartitionMetadata pm;
		pm.db = new PartitionDB(______); // get partition db filename 
		pm.state = PartitionState:PREPARING;
		pm.other_node = mesg_id.node_id;
		g_current_node_state.partition_map[partition_id] = pm;
		savePartitionState(g_owned_partition_state_filename);
	}

	g_current_node_state.partition_map_lock.releaseWRLock(); // 2
	g_current_node_state.state_lock.releaseRDLock(); // 1
	return true;
}

bool HandleUpdatePartitionOwnerRequest(MessageId mesg_id, node_t new_owner, 
	partition_t partition_id)
{
	assert (g_current_node_id != mesg_id.node_id);

	bool success;
	g_cached_partition_table.lock.acquireWRLock(); // 1
	success = handleUpdatePartitionOwner(mesg_id.node_id, new_owner, partition_id);
	g_cached_partition_table.lock.releaseWRLock(); // 1
	return success;
}

bool HandleJoinRequest(MessageId mesg_id, std::string &new_node);
{
	assert (g_current_node_id != mesg_id.node_id);

	node_t node_id = hostToNodeId(new_node);
	assert (node_id == mesg_id.node_id);

	g_current_node_state.cluster_members_lock.acquireWRLock(); // 1
	// handle already joined case
	if (g_current_node_state.cluster_members.find(node_id) 
		== g_current_node_state.cluster_members.end())
	{
		cout << new_node << " " << node_id << " already joined!" << endl;
	}
	else // allow new node to join the cluster
	{
		g_current_node_state.cluster_members[node_id] = new_node;
		g_current_node_state.saveClusterMemberList(g_cluster_member_list_filename);
		cout << new_node << " " << node_id << " added to cluster" << endl;
	}

	g_current_node_state.partition_map_lock.acquireRDLock(); // 2
	std::list<partition_t> partitions_owned;
	for (auto &kv : g_current_node_state.partition_map)
	{
		if (kv.second.state == PartitionState::STABLE)
		{
			partitions_owned.push_back(kv.first);
		}
	}
	g_current_node_state.partition_map_lock.acquireRDLock(); // 2

	g_current_node_state.cluster_members_lock.releaseWRLock(); // 1
	return partitions_owned;
}

bool HandleLeaveRequest(MessageId mesg_id)
{
	assert (g_current_node_id != mesg_id.node_id);

	g_current_node_state.cluster_members_lock.acquireWRLock(); // 1
	// handle already left case
	if (g_current_node_state.cluster_members.find(mesg_id.node_id) 
		== g_current_node_state.cluster_members.end();
	{
		cout << mesg_id.node_id << " already left!" << endl;
	}
	else // remove the node from the cluster
	{
		g_current_node_state.cluster_members.erase(mesg_id.node_id);
		g_current_node_state.saveClusterMemberList(g_cluster_member_list_filename);
		cout << mesg_id.node_id << " left cluster!" << endl;
	}
	g_current_node_state.cluster_members_lock.releaseWRLock(); // 1

	return true;
}

GetResult HandleGetRequest(device_t deviceId, time_t lower_range, time_t upper_range)
{
	GetResult = ret;
	partition_t partition_to_get_from = g_cached_partition_table.getPartition(hash_integer(device_id));

	PartitionMetadata partition_state;
	g_current_node_state.partition_map_lock.acquireRDLock(); // 1
	try
	{
		partition_state = g_current_node_state.partition_map.at(partition_to_get_from);
	}
	catch (const std::out_of_range &e) // this range is not owned by current node!
	{
		cout << "received get on unowned range!" << endl;
		ret.success = false;
		ret.error = DATA_NOT_OWNED;
		g_current_node_state.partition_map_lock.releaseRDLock(); // 1
		return ret;
	}

	switch (partition_state.state)
	{
	case STABLE: // consistent
	case RECEIVED: // still moving, but consistent
		PartitionDB *db = partition_state.db;
		ret.values = db->get(device_id, lower_range, upper_range);
		ret.success = true;
		break;
	case RECEIVING:
		ret.error = DATA_MOVING; // not ready to service reads yet
		ret.success = false;
	case DONATING: // data no longer at this node 
		ret.error = DATA_MOVED;
		ret.moved_to = partition_state.other_node;
		ret.success = false;
		break;
	}
	g_current_node_state.partition_map_lock.releaseRDLock(); // 1

	return ret;
}

GetPartitionTableResult HandleGetPartitionTableRequest(void)
{
	GetPartitionTableResult ret;
	g_current_node_state.state_lock.acquireRDLock(); // 1
	if (g_current_node_state.state == NodeState::JOINING)
	{
		ret.success = false;
		g_current_node_state.state_lock.releaseRDLock(); // 1
	}
	else
	{
		g_current_node_state.state_lock.releaseRDLock(); // 1
	}

	// don't need partition table read lock since the table request
	// is a hint for the Monitor
	ret.num_partitions = g_cached_partition_table.getNumPartitions();
	ret.num_replicas = g_cached_partition_table.getNumReplicas();
	ret.partition_table = g_cached_partition_table.getPartitionTable();
	ret.success = true;
	return ret;
}




