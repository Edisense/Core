
#include <iostream>
#include <fstream>

#include "partition/hash.h"

#include "state.h"

static const int kCounterEpoch = 10000;

using namespace std;

transaction_t NodeStateMachine::getTransactionID(string &filename)
{
	transaction_t ret;
	lock_guard<recursive_mutex>(this->counter_lock);
	ret = counter++;
	if (ret % kCounterEpoch == 0)
	{
		saveNodeState(filename);
	}
	return ret;
}

void NodeStateMachine::saveNodeState(string &filename)
{
	string tmp_file = filename + ".tmp";
	ofstream ofs;
  	ofs.open(tmpFile);

  	switch (state)
  	{
  		case NodeState::STABLE:
  			ofs << "STABLE";
  			break;
  		case NodeState::JOINING:
  			ofs << "JOINING";
  			break;
		case NodeState::LEAVING:
			ofs << "LEAVING";
  			break;
		case NodeState::RECOVERING:
			ofs << "RECOVERING";
  			break;
  	}

  	counter_lock.lock();
  	ofs << "\t" << counter; // flush counter to file
  	counter_lock.unlock();
  	ofs.close();
  	if (rename(tmp_file.c_str(), filename.c_str()) != 0)
  	{
  		throw "failed to rename node state file";
  	}
}

void NodeStateMachine::loadNodeState(string &filename)
{
	ifstream ifs(filename);
	string s;
	ifs >> s;
	if (s == "STABLE")
	{
		state = NodeState::STABLE;
	}
	else if (s == "JOINING")
	{
		state = NodeState::JOINING;
	}
	else if (s == "LEAVING")
	{
		state = NodeState::LEAVING;
	}
	else if (s == "RECOVERING")
	{
		state = NodeState::RECOVERING;
	}
	else
	{
		throw "unrecognized state";
	}
	transaction_t c;
	ifs >> c;
	counter_lock.lock();
	counter = c + counter_epoch; // counter will never be too small
	counter_lock.unlock();
}
	
void NodeStateMachine::savePartitionState(string &filename)
{
	string tmp_file = filename + ".tmp";
	ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : partition_map)
  	{
  		PartitionMetadata pm = kv.second;
  		switch (pm.state)
  		{
 	 		case PartitionState::DONATING:
 	 			ofs << "DONATING\t" << kv.first << "\t" << pm.other_node;
 	 			break;
 	 		case PartitionState::RECEIVING:
 	 			ofs << "RECEIVING\t" << kv.first << "\t" << pm.other_node;
 	 			break;
 	 		case PartitionState::RECEIVED:
 	 		 	ofs << "RECEIVED\t" << kv.first << "\t" << pm.other_node;
 	 			break;
 	 		case PartitionState::STABLE:
 	 		 	ofs << "STABLE\t" << kv.first;
 	 			break;
  		}
  		ofs << endl;
  	}
  	ofs.close();
	if (rename(tmp_file.c_str(), filename.c_str()) != 0)
  	{
  		throw "failed to rename partition state file";
  	}
}

void NodeStateMachine::loadPartitionState(string &filename)
{
	std::map<partition_t, PartitionMetadata> tmp_map;
	ifstream ifs(filename);
	while (ifs)
	{
		string state;
		ifs >> state;
		partition_t partition_id;
		ifs >> partition_id;
		
		PartitionMetadata pm;
		
		if (state == "DONATING")
		{
			pm.state = PartitionState::DONATING;
			ifs >> pm.other_node;
		} 
		else if (state == "RECEIVING")
		{
			pm.state = PartitionState::RECEIVING;
			ifs >> pm.other_node;
		}
		else if (state == "RECEIVED")
		{
			pm.state = PartitionState::RECEIVED;
			ifs >> pm.other_node;
		}
		else 
		{
			pm.state = PartitionState::STABLE;
		}

		if (tmp_map.find(partition_id) != tmp_map.end())
		{
			throw "duplicate partition entry in file";
		}
		tmp_map[partition_id] = pm;
	}
	partition_map = tmp_map;
}

void NodeStateMachine::saveClusterMemberList(string &filename)
{
	string tmp_file = filename + ".tmp";
	ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : cluster_members)
  	{
  		ofs << kv.second << endl;
  	}
  	ofs.close();
  	if (rename(tmp_file.c_str(), filename.c_str()) != 0)
  	{
  		throw "failed to rename cluser member list file";
  	}
}

void NodeStateMachine::loadClusterMemberList(string &filename)
{
	ifstream ifs(filename);
	std::map<node_t, string> node_map;
	while (ifs)
	{
		std::string hostname;
		ifs >> hostname;
		node_t node_id = hostToNodeId(hostname);
		if (node_map.find(node_id) != node_map.end())
		{
			throw "duplicate hostname in file";
		}
		node_map[node_id] = hostname;
	}
	ifs.close();
	cluster_members = node_map;
}