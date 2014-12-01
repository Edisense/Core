#ifndef DB_FILE_TRANSFER
#define DB_FILE_TRANSFER

#include <string>

#include "edisense_types.h"

bool ReceiveDBFile(int sockfd);

bool SendDBFile(std::string &hostname, std::string &filename, partition_t partition_id);

#endif DB_FILE_TRANSFER