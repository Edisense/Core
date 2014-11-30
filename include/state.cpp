
#include <iostream>
#include <fstream>

#include "partition/hash.h"

#include "state.h"

static const int counter_epoch = 10000;

transaction_t NodeState::getTransactionID(std::string &filename)
{
	transaction_t ret;
	std::lock_guard(counter_lock);
	ret = counter++;
	if (ret % counter_epoch == 0)
	{
		saveNodeState(filename);
	}
	return ret;
}

void NodeState::saveNodeState(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);

  	switch (state)
  	{
  		case STABLE:
  			ofs << "STABLE";
  			break;
  		case JOINING:
  			ofs << "JOINING";
  			break;
		case LEAVING:
			ofs << "LEAVING";
  			break;
		case RECOVERING:
			ofs << "RECOVERING";
  			break;
  	}

  	counter_lock.lock();
  	ofs << "\t" << counter; // flush counter to file
  	counter_lock.unlock();
  	ofs.close();
  	if (rename(tmp_file, filename) != 0)
  	{
  		throw "failed to rename node state file";
  	}
}

void NodeState::loadNodeState(std::string &filename)
{
	std::ifstream ifs(filename);
	std::string s;
	ifs >> s;
	if (s == "STABLE")
	{
		state = STABLE;
	}
	else if (s == "JOINING")
	{
		state = JOINING;
	}
	else if (s == "LEAVING")
	{
		state = LEAVING;
	}
	else if (s == "RECOVERING")
	{
		state = RECOVERING;
	}
	else
	{
		throw "unrecognized state";
	}
	transaction_t c;
	ifs >> t;
	counter_lock.lock();
	counter = c + counter_epoch; // counter will never be too small
	counter_lock.unlock();
}
	
void NodeState::savePartitionState(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : partition_map)
  	{
  		struct partition_meta_t pm = kv.second;
  		switch (pm.state)
  		{
 	 		case MIGRATING_FROM:
 	 			ofs << "FROM\t" << kv.first << "\t" << pm.other_node;
 	 			break;
 	 		case MIGRATING_TO:
 	 			ofs << "TO\t" << kv.first << "\t" << pm.other_node;
 	 			break;
 	 		case STABLE:
 	 		 	ofs << "STABLE\t" << kv.first;
 	 			break;
  		}
  		ofs << endl;
  	}
  	ofs.close();
	if (rename(tmp_file, filename) != 0)
  	{
  		throw "failed to rename partition state file";
  	}
}

void NodeState::loadPartitionState(std::string &filename)
{
	std::map<partition_t, struct partition_meta_t> tmp_map;
	std::ifstream ifs(filename);
	while (ifs)
	{
		std::string state;
		ifs >> state;
		partition_t partition_id;
		ifs >> partition_id;
		
		struct partition_meta_t pm;
		
		if (state == "FROM")
		{
			pm.state = MIGRATING_FROM;
			ifs >> pm.other_node;
		} 
		else if (state == "TO")
		{
			pm.state = MIGRATING_TO;
			ifs >> pm.other_node;
		}
		else 
		{
			pm.state = STABLE; 
		}

		if (tmp_map.find(partition_id) != tmp_map.end())
		{
			throw "duplicate partition entry in file";
		}
		tmp_map.insert(partition_id, pm);
	}
	partition_map = tmp_map;
}

void NodeState::saveClusterMemberList(std::string &filename)
{
	std::string tmp_file = filename + ".tmp";
	std::ofstream ofs;
  	ofs.open(tmp_file);
  	for (auto kv : cluster_members)
  	{
  		ofs << kv.second << endl;
  	}
  	ofs.close();
  	if (rename(tmp_file, filename) != 0)
  	{
  		throw "failed to rename cluser member list file";
  	}
}

void NodeState::loadClusterMemberList(std::string &filename)
{
	std::ifstream ifs(filename);
	std::map<node_t, std::string> node_map;
	while (ifs)
	{
		string hostname;
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