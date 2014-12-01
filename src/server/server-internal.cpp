
#include "state.h"
#include "partition_db.h"
#include "server-internal.h"
#include "hash.h"

PutResult handlePutRequest(MessageId mesg_id, device_t device_id, 
	time_t timestamp, time_t expiration, void *data, size_t data_len)
{
	PutResult ret;
	partition_t partition_to_put_into = g_cached_partition_table.getPartition(hash_integer(device_id));

	PartitionMetadata partition_state;
	g_current_node_state.partition_map_lock.acquireRDLock();
	try
	{
		partition_state = g_current_node_state.partition_map.at(partition_to_put_into);
	}
	catch (const std::out_of_range &e) 
	{
		ret.success = false;
		g_current_node_state.partition_map_lock.releaseRDLock();
		return ret;
	}

	switch (partition_state.state)
	{
	case STABLE:
	case MIGRATING_TO:
		PartitionDB *db = partition_state.db;
		ret.success = db->put(device_id, timestamp, expiration, data, data_len);
		ret.moved = false;
		break;
	case MIGRATING_FROM:
		ret.success = false;
		ret.moved = true;
		ret.moved_to = partition_state.other_node;
		break;
	}
	g_current_node_state.partition_map_lock.releaseRDLock();
	return ret;
}

GetResult handleGetRequest(MessageId mesg_id, 
	device_t deviceId, time_t lower_range, time_t upper_range)
{
	GetResult = ret;
	partition_t partition_to_get_from = g_cached_partition_table.getPartition(hash_integer(device_id));

	PartitionMetadata partition_state;
	g_current_node_state.partition_map_lock.acquireRDLock();
	try
	{
		partition_state = g_current_node_state.partition_map.at(partition_to_get_from);
	}
	catch (const std::out_of_range &e) 
	{
		ret.success = false;
		g_current_node_state.partition_map_lock.releaseRDLock();
		return ret;
	}

	switch (partition_state.state)
	{
	case STABLE:
	case MIGRATING_TO:
		PartitionDB *db = partition_state.db;
		ret.values = db->get(device_id, lower_range, upper_range);
		ret.moved = false;
		break;
	case MIGRATING_FROM:
		ret.success = false;
		ret.moved = true;
		ret.moved_to = partition_state.other_node;
		break;
	}

	g_current_node_state.partition_map_lock.releaseRDLock();
	return ret;
}

bool handleJoinRequest(MessageId mesg_id, std::string &new_node);
{
	g_current_node_state.cluster_members_lock.acquireWRLock();
	// handle already joined case
	if (g_current_node_state.cluster_members.find(mesg_id.node_id) 
		== g_current_node_state.cluster_members.end();
	{

	}

	g_current_node_state.cluster_members_lock.releaseWRLock();
	return true;
}

bool handleLeaveRequest(MessageId mesg_id)
{
	g_current_node_state.cluster_members_lock.acquireWRLock();	
	if (g_current_node_state.cluster_members.find(mesg_id.node_id) 
		== g_current_node_state.cluster_members.end();
	{

	}

	g_current_node_state.cluster_members_lock.releaseWRLock();
}




