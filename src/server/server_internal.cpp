#include <cassert>
#include <iostream>
#include "state.h"
#include "global.h"
#include "partition/partition_table.h"
#include "partition/partition_db.h"
#include "util/hash.h"
#include "util/utilization.h"

#include "server_internal.h"


static const float kMaximumUtilizationToAcceptDonation = 0.6;
static const size_t kMinBytesFreeToAcceptDonation = 64 * 1024 * 1024; // 512mb


JoinResult Server::handleJoinRequest(node_t sender, transaction_t tid, std::string &new_node) 
{
  std::cout << "handle join request from " << new_node << std::endl; 
  assert (g_current_node_id != sender);

  node_t node_id = hostToNodeId(new_node);
  assert (node_id == sender);

  JoinResult result;
  result.success = false;
  if (g_current_node_state->state == NodeState::JOINING)
  {
    return result;
  }

  g_current_node_state->cluster_members_lock.acquireWRLock(); // 1

  // handle already joined case
  if (g_current_node_state->cluster_members.find(node_id)
      != g_current_node_state->cluster_members.end()) {
    std::cout << new_node << " " << node_id << " already joined!" << std::endl;
  }
  else // allow new node to join the cluster
  {
    g_current_node_state->cluster_members[node_id] = new_node;
    g_current_node_state->saveClusterMemberList(g_cluster_member_list_filename);
    std::cout << new_node << " " << node_id << " added to cluster" << std::endl;
  }

  // send back the partitions owned by this node
  result.num_partitions = g_cached_partition_table->getNumPartitions();
  result.num_replicas = g_cached_partition_table->getNumReplicas();
  g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 2
  for (auto &kv : g_current_node_state->partitions_owned_map) 
  {
    if (kv.second.state == PartitionState::STABLE) 
    {
      result.partitions.push_back(kv.first);
    }
  }
  g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 2

  g_current_node_state->cluster_members_lock.releaseWRLock(); // 1
  result.success = true;
  return result;
}

bool Server::handleLeaveRequest(node_t sender, transaction_t tid) {
  assert (g_current_node_id != sender);

  g_current_node_state->cluster_members_lock.acquireWRLock(); // 1
  // handle already left case
  if (g_current_node_state->cluster_members.find(sender)
      == g_current_node_state->cluster_members.end()) {
    std::cout << sender << " already left!" << std::endl;
  }
  else // remove the node from the cluster
  {
    g_current_node_state->cluster_members.erase(sender);
    g_current_node_state->saveClusterMemberList(g_cluster_member_list_filename);
    std::cout << sender << " left cluster!" << std::endl;
  }
  g_current_node_state->cluster_members_lock.releaseWRLock(); // 1

  return true;
}

PutResult Server::handlePutRequest(node_t sender, transaction_t tid, device_t deviceId, time_t timestamp, time_t expiry, blob data) 
{
  assert (g_current_node_id != sender);

  std::cout << "Received put request from " << sender << " : " << deviceId << " (device) " << timestamp
      << " (timestamp) " << expiry << " (expiry) " << std::endl;

  PutResult ret;
  partition_t partition_to_put_into = g_cached_partition_table->getPartition(hash_integer(deviceId));

  PartitionMetadata partition_state;
  g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 1

  try {
    partition_state = g_current_node_state->partitions_owned_map.at(partition_to_put_into);
  }
  catch (const std::out_of_range &e) // this range is not owned by current node!
  {
    std::cout << "received put on unowned range!" << std::endl;
    ret.status = DATA_NOT_OWNED;
    g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1
    return ret;
  }

  if (partition_state.state == PartitionState::STABLE
      || partition_state.state == PartitionState::RECEIVED) 
  {
    std::cout << "putting into db" << std::endl;
    PartitionDB *db = partition_state.db;
    bool success = db->put(deviceId, timestamp, expiry, &data[0], data.size());
    if (!success) 
      ret.status = DB_ERROR;
    else 
      success = ret.status = SUCCESS;
  }
  else if (partition_state.state == PartitionState::RECEIVING) // not ready to handle put request yet
  {
    std::cout << "currently receiving" << std::endl;
    ret.status = DATA_MOVING;
  }
  else // (partition_state.state == PartitionState::DONATING)
  {
    std::cout << "donating the partition" << std::endl;
    ret.status = DATA_MOVED;
    ret.moved_to = partition_state.other_node;
  }
  g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1

  std::cout << "Success: " << (ret.status == SUCCESS) << std::endl;
  return ret;
}

std::list<std::string> *Server::handleLocateRequest(device_t deviceId)
{
  std::list<std::string> *owners = new std::list<std::string>();
  partition_t partition_to_get_from = g_cached_partition_table->getPartition(hash_integer(deviceId));
  int num_replicas = g_cached_partition_table->getNumReplicas();
  g_cached_partition_table->lock.acquireRDLock(); // 1
  g_current_node_state->cluster_members_lock.acquireRDLock(); // 2
  node_t *owner_node_ids = g_cached_partition_table->getPartitionOwners(partition_to_get_from);

  // translate node_ids to actual hostnames
  for (int i = 0; i < num_replicas; i++)
  {
    owners->push_back(g_current_node_state->cluster_members[owner_node_ids[i]]);
  }

  g_current_node_state->cluster_members_lock.releaseRDLock();
  g_cached_partition_table->lock.releaseRDLock();

  return owners;
}

GetResult Server::handleGetRequest(transaction_t tid, device_t deviceId, time_t begin, time_t end) 
{
  std::cout << "handle get request on " << deviceId << std::endl;

  GetResult ret;
  partition_t partition_to_get_from = g_cached_partition_table->getPartition(hash_integer(deviceId));

  PartitionMetadata partition_state;
  g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 1
  try {
    partition_state = g_current_node_state->partitions_owned_map.at(partition_to_get_from);
  }
  catch (const std::out_of_range &e) // this range is not owned by current node!
  {
    std::cout << "received get on unowned range!" << std::endl;
    ret.status = DATA_NOT_OWNED;
    g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1
    return ret;
  }

  if (partition_state.state == PartitionState::STABLE
      || partition_state.state == PartitionState::RECEIVED) {
    PartitionDB *db = partition_state.db;
    ret.values = db->get(deviceId, begin, end);
    ret.status = SUCCESS;
  }
  else if (partition_state.state == PartitionState::RECEIVING) {
    ret.status = DATA_MOVING; // not ready to service reads yet
  }
  else // DONATING: data no longer at this node
  {
    ret.status = DATA_MOVED;
    ret.moved_to = partition_state.other_node;
  }
  g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1

  return ret;
}

bool Server::handleUpdatePartitionOwner(node_t sender, transaction_t tid, node_t newOwner, partition_t partition) 
{
  std::cout << "handle update partition owner for " << partition << " new owner " << newOwner << " old owner " << sender << std::endl;
  assert (g_current_node_id != sender);
  if (g_current_node_state->state == NodeState::JOINING)
  {
    return false;
  }

  bool success;
  g_cached_partition_table->lock.acquireWRLock(); // 1
  success = g_cached_partition_table->updatePartitionOwner(sender, newOwner, partition);
  g_cached_partition_table->lock.releaseWRLock(); // 1
  return success;
}

CanReceiveResult Server::handleCanReceiveRequest(node_t sender, transaction_t tid, partition_t partition_id) 
{
  assert (g_current_node_id != sender);

  std::cout << "received CanReceive request from " << sender << std::endl;
  CanReceiveResult ret;
  uint64_t used = ComputeNodeUtilization();
  ret.free = g_local_disk_limit_in_bytes - used;
  ret.util = used / (float) g_local_disk_limit_in_bytes;
  if (ret.util > kMaximumUtilizationToAcceptDonation || ret.free < kMinBytesFreeToAcceptDonation) 
  {
    std::cout << "reject can receive request " << partition_id << " from " << sender << " due to utilization" << std::endl;
    ret.can_recv = false;
    return ret;
  }

  // don't need the state lock since it is a hint
  if (g_current_node_state->state != NodeState::STABLE) // can only accept if stable
  {
    std::cout << "reject can receive request " << partition_id << " from " << sender << " due to node state" << std::endl;
    ret.can_recv = false;
    return ret;
  }

  g_current_node_state->partitions_owned_map_lock.acquireRDLock(); // 1
  if (g_current_node_state->partitions_owned_map.find(partition_id)
      != g_current_node_state->partitions_owned_map.end()) // check if storing partition already
  {
    std::cout << "reject can receive request " << partition_id << " from " << sender << " due to already owned" << std::endl;
    ret.can_recv = false;
    g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1
    return ret;
  }
  else {
    g_current_node_state->partitions_owned_map_lock.releaseRDLock(); // 1
  }

  std::cout << "accept can receive request " << partition_id << " from " << sender << std::endl;
  ret.can_recv = true;
  return ret;
}

bool Server::handleCommitReceiveRequest(node_t sender, transaction_t tid, partition_t partition_id) 
{
  assert (g_current_node_id != sender);

  size_t used = ComputeNodeUtilization(); // TODO what if we over commit?
  size_t free_space = g_local_disk_limit_in_bytes - used;
  float util = used / (float) g_local_disk_limit_in_bytes;
  if (util > kMaximumUtilizationToAcceptDonation || free_space < kMinBytesFreeToAcceptDonation) 
  {
    std::cout << "reject commit receive request " << partition_id << " from " << sender << " due to utilization" << std::endl;
    return false;
  }

  g_current_node_state->state_lock.acquireRDLock(); // 1
  if (g_current_node_state->state != NodeState::STABLE) // can only accept if stable
  {
    std::cout << "reject commit receive request " << partition_id << " from " << sender << " due to node state" << std::endl;
    g_current_node_state->state_lock.releaseRDLock(); // 1
    return false;
  }

  g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 2
  if (g_current_node_state->partitions_owned_map.find(partition_id)
      != g_current_node_state->partitions_owned_map.end()) // check if storing partition already
  {
    PartitionMetadata pm = g_current_node_state->partitions_owned_map[partition_id];
    if (pm.state == PartitionState::RECEIVING
        && pm.other_node == sender) // retransmission of comfirm receive
    {
      // reply true
    }
    else // already own or receiving the partition from another source
    {
      std::cout << "reject commit receive request " << partition_id << " from " << sender << " due to already owned" << std::endl;
      g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 2
      g_current_node_state->state_lock.releaseRDLock(); // 1
      return false;
    }
  }
  else // add to partition map
  {
    PartitionMetadata pm;
    pm.db = NULL; // get partition db filename
    pm.state = PartitionState::RECEIVING;
    pm.other_node = sender;
    g_current_node_state->partitions_owned_map[partition_id] = pm;
    g_current_node_state->savePartitionState(g_owned_partition_state_filename);
  }
  g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 2
  g_current_node_state->state_lock.releaseRDLock(); // 1

  std::cout << "accept commit receive request " << partition_id << " from " << sender << std::endl;

  // update cached partition to node mapping
  g_cached_partition_table->lock.acquireWRLock();
  g_cached_partition_table->updatePartitionOwner(sender, g_current_node_id, partition_id);
  g_cached_partition_table->lock.releaseWRLock();
  return true;
}

bool Server::handleCommitAsStableRequest(node_t sender, transaction_t tid, partition_t partition_id) 
{
  assert (g_current_node_id != sender);
  g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1
  // Cannot commit as stable
  if (g_current_node_state->partitions_owned_map.find(partition_id) == g_current_node_state->partitions_owned_map.end())
  {
    std::cout << "duplicate commit as stable request " << partition_id << " from " << sender << std::endl;
    g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1
    return true;
  }
  std::cout << "accept commit as stable request " << partition_id << " from " << sender << std::endl;
  g_current_node_state->partitions_owned_map[partition_id].state = PartitionState::STABLE;
  g_current_node_state->savePartitionState(g_owned_partition_state_filename);
  g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1
  return true;
}
