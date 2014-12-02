#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

#include <list>

#include "state.h"

#include "edisense_types.h"

/* Called by other nodes */

PutResult HandlePutRequest(MessageId mesg_id, device_t device_id, 
	time_t timestamp, time_t expiration, void *data, size_t data_len);

CanReceiveResult HandleCanReceiveRequest(MessageId mesg_id, partition_t partition_id);

bool HandleCommitReceiveRequest(MessageId mesg_id, partition_t partition_id);

bool HandleCommitAsStableRequest(MessageId mesg_id, partition_t partition_id);

bool HandleUpdatePartitionOwnerRequest(MessageId mesg_id, node_t new_owner, 
	partition_t partition_id);

std::list<partition_t> HandleJoinRequest(MessageId mesg_id, std::string &new_node);

bool HandleLeaveRequest(MessageId mesg_id);

/* Called by Monitor -- do not need mesg_id */

GetResult HandleGetRequest(device_t deviceId, time_t lower_range, time_t upper_range);

GetPartitionTableResult HandleGetPartitionTableRequest();

#endif /* SERVER_INTERNAL_H */