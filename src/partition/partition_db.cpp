
#include <mutex>
#include <cassert>
#include <cstring>
#include <sys/stat.h>

#include <list>
#include <unistd.h>

#include "global.h"

#include "partition_db.h"

std::string GetPartitionDBFilename(partition_t p)
{
	return g_db_files_dirname + "/" + std::to_string(p) + ".sqllite";
}

static int createTableCallback(void *NotUsed, int argc, char **argv, char **azColName){
	printf("Callback called: create table\n"); 
	int i;
	char const * value;
	for(i=0; i<argc; i++) {
		if (argv[i]) {
			value = argv[i];
		} else {
			value = "NULL";
		}
		printf("%s = %s\n", azColName[i], value);
	}
	printf("\n");
	return 0;
}

PartitionDB::PartitionDB(const std::string &dbName)
{
	filename = dbName;
	fprintf(stderr, "Open database: %s\n", filename.c_str());
	if (sqlite3_open(filename.c_str(), &db) != SQLITE_OK)
	{
		fprintf(stderr, "%s\n", sqlite3_errmsg(db));
		perror("Can't open database!");
		exit(1);
	}
	fprintf(stderr, "Opened database successfully\n");

	char *sql = "CREATE TABLE IF NOT EXISTS stored_values( " \
		"row_id INT PRIMARY KEY," \
		"device_id 	INT NOT NULL," \
		"timestamp 	INT	NOT NULL," \
		"expiration INT NOT NULL," \
		"data	BLOB	NOT NULL );";

	char *errMsg = NULL;
	if(sqlite3_exec(db, sql, createTableCallback, 0, &errMsg) != SQLITE_OK) 
	{
		fprintf(stderr, "SQL error: %s\n", errMsg);
		sqlite3_free(errMsg);
		perror("Table not found and can't create it!");
		exit(1);
	}
}

PartitionDB::~PartitionDB()
{
	sqlite3_close(db);
}

static bool compareBuffer(char *buf1, char *buf2, size_t len)
{
	for (int i = 0; i < len; i++)
	{
		if (buf1[i] != buf2[i]) return false;
	}
	return true;
}

bool PartitionDB::put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t datalen)
{
	assert(datalen < kMaxDataLen);

	int result;
	sqlite3_stmt *stmt;
	char sql[256];
	int sql_len;

	std::lock_guard<std::mutex> lg(db_lock);	

	sql_len = sprintf(sql, "SELECT * FROM stored_values WHERE (device_id = %d AND timestamp = %ld);", device_id, timestamp);
	stmt = NULL;
	result = sqlite3_prepare_v2(db, sql, sql_len, &stmt, NULL);
	if (result != SQLITE_OK)
	{
		return false;
	}

	/* 
	 * TODO : Duplicate handling not fully implemented 
	 */
	while (true)
	{
		result = sqlite3_step(stmt);
		if (result == SQLITE_DONE)
		{
			break;
		}
		else if (result == SQLITE_ROW)
		{
			time_t row_expiration = sqlite3_column_int(stmt, 3);
			size_t row_data_size = sqlite3_column_bytes(stmt, 4);
			if (row_data_size != datalen)
			{
				sqlite3_finalize(stmt);
				return false;
			}
			void const *row_data = sqlite3_column_blob(stmt, 4);
			assert(row_data);
			if (!compareBuffer)
			{
				sqlite3_finalize(stmt);
				return false;
			}
			sqlite3_finalize(stmt);
			return true;
		}
		else if (result == SQLITE_BUSY)
		{
			usleep(1000);
		} 
		else 
		{
			sqlite3_finalize(stmt);
			return false;
		}
	}
	sqlite3_finalize(stmt);

	stmt = NULL;
	sql_len = sprintf(sql, "INSERT INTO stored_values VALUES(NULL, %d, %ld, %ld, ?);", device_id, timestamp, expiration);
	result = sqlite3_prepare_v2(db, sql, sql_len, &stmt, NULL);
	if (result != SQLITE_OK)
	{
		return false;
	}

	result = sqlite3_bind_blob(stmt, 1, data, datalen, SQLITE_STATIC);
	if (result != SQLITE_OK)
	{
		sqlite3_finalize(stmt);
		return false;
	}

	result = sqlite3_step(stmt);
	bool success = (result == SQLITE_DONE);

	sqlite3_finalize(stmt);
	return success;
}

std::list<Data> * PartitionDB::get(device_t device_id, time_t min_timestamp, time_t max_timestamp)
{
	char sql[256];
	int sql_len;
	int result;
	sqlite3_stmt *stmt;

	std::list<Data> *ret = new std::list<Data>();

	std::lock_guard<std::mutex> lg(db_lock);	

	sql_len = sprintf(sql, "SELECT * FROM stored_values " \
		"WHERE (device_id = %d AND timestamp BETWEEN %ld AND %ld) " \ 
		"ORDER BY timestamp ASC;", device_id, min_timestamp, max_timestamp);
	result = sqlite3_prepare_v2(db, sql, sql_len, &stmt, NULL);
	if (result != SQLITE_OK)
	{
		sqlite3_finalize(stmt);
		return nullptr; // TODO Throw exception or return empty list?
	}

	while (true)
	{
		result = sqlite3_step(stmt);
		if (result == SQLITE_DONE)
		{
			break;
		}
		else if (result == SQLITE_ROW)
		{
			time_t timestamp = sqlite3_column_int(stmt, 2);
			time_t expiration = sqlite3_column_int(stmt, 3);
			size_t data_size = sqlite3_column_bytes(stmt, 4);
			assert(data_size < kMaxDataLen);
			char const *data = static_cast<char const *>(sqlite3_column_blob(stmt, 4));

			Data d;
			d.timestamp = timestamp;
			d.expiration = expiration;
                        blob point(data, data + data_size);
			d.data = point;
			ret->push_back(d);
		}
		else if (result == SQLITE_BUSY)
		{
			usleep(1000);
		} 
		else 
		{
			sqlite3_finalize(stmt);
			throw "sqlite db error";
		}
	}
	sqlite3_finalize(stmt);

	return ret;
}

bool PartitionDB::remove(time_t timestamp)
{
	char sql[256];
	int sql_len;
	int result;
	sqlite3_stmt *stmt;
	
	std::lock_guard<std::mutex> lg(db_lock);	

	sql_len = sprintf(sql, "DELETE FROM stored_values WHERE (timestamp < %ld);", timestamp);
	result = sqlite3_prepare_v2(db, sql, sql_len, &stmt, NULL);
	if (result != SQLITE_OK)
	{
		sqlite3_finalize(stmt);
		return false;
	}

	result = sqlite3_step(stmt);
	bool success = (result == SQLITE_DONE);

	sqlite3_finalize(stmt);
	return success;
}

std::list<device_t> * PartitionDB::getDevices(void)
{
	int result;
	sqlite3_stmt *stmt;
	std::list<device_t> *ret = new std::list<device_t>;

	std::lock_guard<std::mutex> lg(db_lock);	

	result = sqlite3_prepare_v2(db, "SELECT DISTINCT device_id FROM stored_values;", -1, &stmt, NULL);
	if (result != SQLITE_OK)
	{
		sqlite3_finalize(stmt);
		return nullptr; // TODO Empty list or throw exception?
	}

	while (true)
	{
		result = sqlite3_step(stmt);
		if (result == SQLITE_DONE)
		{
			break;
		}
		else if (result == SQLITE_ROW)
		{
			device_t d = sqlite3_column_int(stmt, 0);
			ret->push_back(d);
		}
		else if (result == SQLITE_BUSY)
		{
			usleep(1000);
		} 
		else 
		{
			sqlite3_finalize(stmt);
			throw "sqlite db error";
		}
	}
	sqlite3_finalize(stmt);

	return ret;
}

long long PartitionDB::size(void)
{
	struct stat sb;
	if (lstat(filename.c_str(), &sb) != 0)
	{
		fprintf(stderr, "failed to stat file %s\n", filename.c_str());
		throw "can't stat db file";
	}
	return sb.st_size;
}

