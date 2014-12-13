
#include "rebalance_log.h"

#include <fstream>
#include <cassert>
#include <map>

using namespace std;

RebalanceLog::RebalanceLog(string &logfile)
{
	filename = logfile;
}

void RebalanceLog::logPresend(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs;
	ofs.open(filename, ios::app);
	if (ofs.fail()) perror("failed to open");
	ofs << "PRESEND" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAbort(partition_t partition_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs;
	ofs.open(filename, ios::app);
	if (ofs.fail()) perror("failed to open");
	ofs << "ABORT" << "\t" << partition_id << endl; 
	ofs.close();
}

void RebalanceLog::logCommit(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs;
	ofs.open(filename, ios::app);
	if (ofs.fail()) perror("failed to open");
	ofs << "COMMIT" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logAckedAndTransferred(partition_t partition_id, node_t node_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs;
	ofs.open(filename, ios::app);
	if (ofs.fail()) perror("failed to open:");
	ofs << "ACKED" << "\t" << partition_id << "\t" << node_id << endl; 
	ofs.close();
}

void RebalanceLog::logComplete(partition_t partition_id)
{
	lock_guard<mutex> g(lock);
	ofstream ofs;
	ofs.open(filename, ios::app);
	if (ofs.fail()) perror("failed to open:");
	ofs << "COMPLETE" << "\t" << partition_id << endl; 
	ofs.close();
}

list<PendingDonate> RebalanceLog::parseLog()
{
	lock_guard<mutex> g(lock);
	ifstream ifs;
	ifs.open(filename);

	map<partition_t, PendingDonate> partition_to_status;

	while (!ifs.eof())
	{
		std::string state;
		ifs >> state;
		if (ifs.fail()) break;

		if (state == "PRESEND")
		{
			partition_t partition_id;
			node_t node_id;
			ifs >> partition_id >> node_id;
			PendingDonate &pd = partition_to_status[partition_id];
			pd.status = DS_PRESEND;
			pd.recipient = node_id;
			pd.partition_id = partition_id;
		}
		else if (state == "COMMIT")
		{
			partition_t partition_id;
			node_t node_id;
			ifs >> partition_id >> node_id;
			PendingDonate &pd = partition_to_status[partition_id];
			assert (pd.recipient == node_id);
			assert (pd.status == DS_PRESEND);
			pd.status = DS_COMMIT;
			pd.partition_id = partition_id;
		}
		else if (state == "ACKED")
		{
			partition_t partition_id;
			node_t node_id;
			ifs >> partition_id >> node_id;
			PendingDonate &pd = partition_to_status[partition_id];
			assert (pd.recipient == node_id);
			assert (pd.status == DS_COMMIT);
			pd.status = DS_ACKED;
			pd.partition_id = partition_id;
		}
		else if (state == "COMPLETE" || state == "ABORT")
		{
			partition_t partition_id;
			ifs >> partition_id;
			partition_to_status.erase(partition_id);
		}
		else
		{
			cerr << "found garbage in logfile" << state << endl;
		}
	}

	list<PendingDonate> ret;
	for (auto &kv : partition_to_status)
	{
		ret.push_back(kv.second);
	}
	return ret;
}