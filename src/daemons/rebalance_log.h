#ifndef REBALANCE_LOG_H
#define REBALANCE_LOG_H

#include <mutex>
#include <string>
#include <iostream>

#include "edisense_types.h"

using namespace std;

class RebalanceLog 
{
public:
	RebalanceLog(string &filename);
	void logPresend(partition_t partition_id, node_t node_id);
	void logAbort(partition_t partition_id, node_t node_id);
	void logCommit(partition_t partition_id, node_t node_id);
	void logAckedAndTransferred(partition_t partition_id);
	void logComplete(partition_t partition_id);
private:
	string filename;
	mutex lock;
};

#endif /* REBALANCE_LOG_H */