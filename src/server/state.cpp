
#include <iostream>
#include <fstream>

#include "partition/partition_db.h"
#include "util/hash.h"

#include "state.h"
#include "global.h"

static const int kCounterEpoch = 10000;

transaction_t NodeStateMachine::getTransactionID()
{
	transaction_t ret;
	std::lock_guard<std::recursive_mutex>(this->counter_lock);
	ret = counter++;
	if (ret % kCounterEpoch == 0)
	{
		saveNodeState(g_current_node_state_filename);
	}
	return ret;
}

void NodeStateMachine::saveNodeState(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);

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

void NodeStateMachine::loadNodeState(std::string &filename)
{
	std::ifstream ifs(filename);
	std::string s;
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
	else
	{
		throw "unrecognized state";
	}
	transaction_t c;
	ifs >> c;
	counter_lock.lock();
	counter = c + kCounterEpoch; // counter will never be too small
	counter_lock.unlock();
	ifs.close();
}
	
void NodeStateMachine::savePartitionState(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : partitions_owned_map)
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
  		ofs << std::endl;
  	}
  	ofs.close();
	if (rename(tmp_file.c_str(), filename.c_str()) != 0)
  	{
  		throw "failed to rename partition state file";
  	}
}

void NodeStateMachine::loadPartitionState(std::string &filename)
{
	std::map<partition_t, PartitionMetadata> tmp_map;
	std::ifstream ifs(filename);
	while (ifs)
	{
		std::string state;
		ifs >> state;
		partition_t partition_id;
		ifs >> partition_id;
		
		PartitionMetadata pm;
		
		if (state == "DONATING")
		{
			pm.state = PartitionState::DONATING;
			pm.db = NULL;
			ifs >> pm.other_node;
		} 
		else if (state == "RECEIVING")
		{
			pm.state = PartitionState::RECEIVING;
			pm.db = NULL;
			ifs >> pm.other_node;
		}
		else if (state == "RECEIVED")
		{
			pm.state = PartitionState::RECEIVED;
			pm.db = new PartitionDB(GetPartitionDBFilename(partition_id));
			ifs >> pm.other_node;
		}
		else 
		{
			pm.state = PartitionState::STABLE;
			pm.db = new PartitionDB(GetPartitionDBFilename(partition_id));
		}

		if (tmp_map.find(partition_id) != tmp_map.end())
		{
			throw "duplicate partition entry in file";
		}
		tmp_map[partition_id] = pm;
	}
	partitions_owned_map = tmp_map;
	ifs.close();
}

void NodeStateMachine::saveClusterMemberList(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : cluster_members)
  	{
  		ofs << kv.second << std::endl;
  	}
  	ofs.close();
  	if (rename(tmp_file.c_str(), filename.c_str()) != 0)
  	{
  		throw "failed to rename cluser member list file";
  	}
}

void NodeStateMachine::loadClusterMemberList(std::string &filename)
{
	std::ifstream ifs(filename);
	std::map<node_t, std::string> node_map;
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