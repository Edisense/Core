
#include "edisense_types.h"

// receiving nodes is the list of node_t that must participate in the
// put, tid is a predetermined transaction_id
void SendPutRequest(transaction_t tid, std::list<node_t> receiving_nodes, 
	device_t device_id, time_t timestamp, time_t expiration, 
	void *data, size_t data_len);

std::list<PutResult> WaitForPutResult(transaction_t tid, int timeout);

// receiving nodes is the list of node_t where the data resides, tid 
// is a predetermined transaction_id
void SendGetRequest(transaction_t tid, std::list<node_t> receiving_nodes, 
	device_t deviceId, time_t lower_range, time_t upper_range);

// one idea is to return as soon as one node in the get replies? 
// then we're doing the eventual consistency model
GetResult WaitForGetResult(transaction_t tid, int timeout);

// broadcast to the list of receiving nodes
void SendUpdatePartitionOwner(transaction_t tid, std::list<node_t> receiving_nodes, 
	node_t new_owner, partition_t partition_id); 

// get list of nodes that acked the update with true
std::list<node_t> WaitForUpdatePartitionOwnerResult(transaction_t tid, int timeout);

// broadcast to the list of receiving nodes
void SendJoinRequest(transaction_t tid, std::list<node_t> receiving_nodes);

// get list of nodes that acked the join with true
std::list<node_t> WaitForJoinResult(transaction_t tid, int timeout);

// broadcast to the list of receiving nodes
void SendLeaveRequest(transaction_t tid, std::list<node_t> receiving_nodes);

// get list of nodes that acked the leave with true
std::list<node_t> WaitForLeaveResult(transaction_t tid, int timeout);