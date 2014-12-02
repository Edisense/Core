
#include <future>

#include "edisense_types.h"

virtual void FreeTransaction(transaction_t tid) = 0;

// receiving nodes is the list of hostnames that must participate in the
// put, tid is a predetermined transaction_id
virtual std::future<std::list<std::pair<node_t, PutResult> > SendPutRequest(transaction_t tid, 
	std::list<string> &receiving_nodes, device_t device_id, time_t timestamp, time_t expiration, 
	void *data, size_t data_len) = 0;

// receiving nodes is the list of hostnames where the data resides, tid 
// is a predetermined transaction_id
virtual std::future<std::pair<node_t, GetResult> > SendGetRequest(transaction_t tid, std::list<string> 
	&receiving_nodes, device_t deviceId, time_t lower_range, time_t upper_range) = 0;

// broadcast to the list of receiving nodes, returns list of nodes that returned true
virtual std::future<std::list<node_t> > SendUpdatePartitionOwner(transaction_t tid, std::list<string> 
	&receiving_nodes, node_t new_owner, partition_t partition_id) = 0; 

// send to recipents, which is a list of hostnames
virtual std::future<std::list<std::pair<node_t, CanReceiveResult> > > SendCanReceiveRequest(transaction_t tid, 
	std::list<string> &receiving_nodes, partition_t partition_id) = 0;

// send to recipent, which is a hostname
virtual std::future<bool> SendCommitReceiveRequest(transaction_t tid, std::string &recipient, 
	partition_t partition_id) = 0;

// send to recipent, which is a hostname
virtual std::future<bool> SendCommitAsStableRequest(transaction_t tid, std::string &recipient, 
	partition_t partition_id) = 0;

// receiving nodes, no longer broadcast
virtual std::future<std::list<partition_t> > SendJoinRequest(transaction_t tid) = 0;

// receiving nodes, no longer broadcast
virtual std::future<bool> SendLeaveRequest(transaction_t tid) = 0;

virtual std::future<GetPartitionTableResult> HandleGetPartitionTableRequest(transaction_t tid, 
	std::string &recipient) = 0;