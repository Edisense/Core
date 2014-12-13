#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

#include <list>
#include <member.h>

#include "state.h"

#include "edisense_types.h"

/* Called by other nodes */

class Server : public edisense_comms::MemberServer {

public:

private:
	virtual GetResult handleGetRequest(transaction_t tid, device_t deviceId, time_t begin, time_t end) override;

	virtual PutResult handlePutRequest(node_t sender, transaction_t tid, device_t deviceId, time_t timestamp, time_t expiry, blob point) override;

	virtual bool handleUpdatePartitionOwner(node_t sender, transaction_t tid, node_t newOwner, partition_t partition) override;

	virtual CanReceiveResult handleCanReceiveRequest(node_t sender, transaction_t tid, partition_t partition_id) override;

	virtual bool handleCommitReceiveRequest(node_t sender, transaction_t tid, partition_t partition_id) override;

	virtual bool handleCommitAsStableRequest(node_t sender, transaction_t tid, partition_t partition_id) override;

	virtual std::list<std::string> *handleLocateRequest(device_t deviceId) override;

	virtual JoinResult handleJoinRequest(node_t sender, transaction_t tid, std::string &new_node) override;

	virtual bool handleLeaveRequest(node_t sender, transaction_t tid);
};



#endif /* SERVER_INTERNAL_H */

