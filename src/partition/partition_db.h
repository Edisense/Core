#ifndef PARTITION_DB_H
#define PARTITION_DB_H

#include <ctime>
#include <mutex>
#include <sqlite3.h> 
#include <string>

#include "edisense_types.h" // definition of kMaxDataLen

// Standardize the partition filenames
std::string GetPartitionDBFilename(partition_t p);

class PartitionDB
{
public:
	PartitionDB(const std::string &filename);
	~PartitionDB();
	bool put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t datalen);
	std::list <Data> * get(device_t device_id, time_t min_timestamp, time_t max_timestamp);
	bool remove(time_t timestamp);
	std::list <device_t> * getDevices(void);
	long long size(void);
private:
	sqlite3 *db;
	std::mutex db_lock;
	std::string filename;
};

#endif /* PARTITION_DB_H */