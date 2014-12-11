
#include "rebalance_log.h"

#include <fstream>

using namespace std;

RebalanceLog::RebalanceLog(string &filename)
{
	filename = filename;
}

void RebalanceLog::logPresend(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs (filename, ofstream::out|ofstream::app);
	ofs << "PRESEND" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAbort(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs (filename, ofstream::out|ofstream::app);
	ofs << "ABORT" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logCommit(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs (filename, ofstream::out|ofstream::app);
	ofs << "COMMIT" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAckedAndTransferred(partition_t partition_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs (filename, ofstream::out|ofstream::app);
	ofs << "ACKED" << "\t" << partition_id << endl; 
	ofs.close();
}

void RebalanceLog::logComplete(partition_t partition_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs (filename, ofstream::out|ofstream::app);
	ofs << "COMPLETE" << "\t" << partition_id << endl; 
	ofs.close();
}