
#include "rebalance_log.h"

RebalanceLog::RebalanceLog(string &filename)
{
	filename = filename;
}

void RebalanceLog::logPresend(partition_t partition_id, node_t node_id)
{
	lock_guard g(lock);
	ofstream ofs (filename, std::ofstream::out|std::ofstream::app);
	ofs << "PRESEND" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAbort(partition_t partition_id, node_t node_id)
{
	lock_guard g(lock);
	ofstream ofs (filename, std::ofstream::out|std::ofstream::app);
	ofs << "ABORT" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logCommit(partition_t partition_id, node_t node_id)
{
	lock_guard g(lock);
	ofstream ofs (filename, std::ofstream::out|std::ofstream::app);
	ofs << "COMMIT" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAckedAndTransferred(partition_t partition_id)
{
	lock_guard g(lock);
	ofstream ofs (filename, std::ofstream::out|std::ofstream::app);
	ofs << "ACKED" << "\t" << partition_id << endl; 
	ofs.close();
}

void RebalanceLog::logComplete(partition_t partition_id)
{
	lock_guard g(lock);
	ofstream ofs (filename, std::ofstream::out|std::ofstream::app);
	ofs << "COMPLETE" << "\t" << partition_id << endl; 
	ofs.close();
}