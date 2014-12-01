#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

#include <list>

#include "include/state.h"

#include "edisense-types.h"

enum ErrorType
{
	DATA_MOVED = 0,
	DATA_NOT_OWNED = 1,
	DATA_MOVING = 2,
	DB_ERROR = 3
};

/* Called by other nodes */

typedef struct PutResult
{
	bool success;
	ErrorType error;
	node_t moved_to;
} PutResult;

PutResult HandlePutRequest(MessageId mesg_id, device_t device_id, 
	time_t timestamp, time_t expiration, void *data, size_t data_len);

typedef struct CanReceiveResult 
{
	bool can_recv;
	float util;
	size_t free;
} CanReceiveResult;

CanReceiveResult HandleCanReceiveRequest(MessageId mesg_id, partition_t partition_id);

bool HandleCommitReceiveRequest(MessageId mesg_id, partition_t partition_id);

bool HandleCommitAsStableRequest(MessageId mesg_id, partition_t partition_id);

bool HandleUpdatePartitionOwnerRequest(MessageId mesg_id, node_t new_owner, 
	partition_t partition_id);

std::list<partition_t> HandleJoinRequest(MessageId mesg_id, std::string &new_node);

bool HandleLeaveRequest(MessageId mesg_id);

typedef struct GetPartitionTableResult
{
	bool success;
	int num_partitions;
	int num_replicas;
	partition_t *partition_table; // DO NOT WRITE TO OR FREE THIS!!!
} GetPartitionTableResult;

/* Called by Monitor -- do not need mesg_id */

typedef struct GetResult
{
	bool success;
	ErrorType error;
	std::list<struct data> *values; 
	node_t moved_to;
} GetResult;

GetResult HandleGetRequest(device_t deviceId, time_t lower_range, time_t upper_range);

GetPartitionTableResult HandleGetPartitionTableRequest();

#endif /* SERVER_INTERNAL_H */