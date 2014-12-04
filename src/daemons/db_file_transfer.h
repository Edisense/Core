#ifndef DB_FILE_TRANSFER
#define DB_FILE_TRANSFER

#include <string>

#include "edisense_types.h"

bool ReceiveDBFile(int sockfd);

bool SendDBFile(int sockfd, const std::string &hostname, const std::string &filename, partition_t partition_id);

#endif /* DB_FILE_TRANSFER */