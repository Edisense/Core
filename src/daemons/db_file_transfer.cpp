// citation: send,recv file 
// from http://stackoverflow.com/questions/25634483/send-binary-file-over-tcp-ip-connection

#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <algorithm>

#include "global.h"
#include "state.h"

#include "util/socket-util.h"
#include "partition/partition_db.h"

#include "db_file_transfer.h"

static bool senddata(int sock, void *buf, int buflen)
{
    unsigned char *pbuf = (unsigned char *) buf;

    while (buflen > 0)
    {
        int num = send(sock, pbuf, buflen, 0);
        if (num == -1)
        {
            return false;
        }

        pbuf += num;
        buflen -= num;
    }

    return true;
}

static bool sendlong(int sock, long value)
{
    value = htonl(value);
    return senddata(sock, &value, sizeof(value));
}

static bool sendfile(int sock, long partition_id, FILE *f)
{
    fseek(f, 0, SEEK_END);
    long filesize = ftell(f);
    rewind(f);
    if (filesize == EOF)
        return false;
    if (!sendlong(sock, filesize))
        return false;
    if (!sendlong(sock, filesize))
        return false;
    if (filesize > 0)
    {
        char buffer[1024];
        do
        {
            size_t num = std::min<long int>(filesize, sizeof(buffer));
            num = fread(buffer, 1, num, f);
            if (num < 1)
                return false;
            if (!senddata(sock, buffer, num))
                return false;
            filesize -= num;
        }
        while (filesize > 0);
    }
    return true;
}

static bool readdata(int sock, void *buf, int buflen)
{
    unsigned char *pbuf = (unsigned char *) buf;

    while (buflen > 0)
    {
        int num = recv(sock, pbuf, buflen, 0);
        if (num == -1)
        {
            return false;
        }
        else if (num == 0)
            return false;

        pbuf += num;
        buflen -= num;
    }

    return true;
}

static bool readlong(int sock, long *value)
{
    if (!readdata(sock, value, sizeof(value)))
        return false;
    *value = ntohl(*value);
    return true;
}

static const long kReadFileFailure = -1;

static long readfile(int sock, FILE *f)
{
	long partition_id;
	if (!readlong(sock, &partition_id))
        return kReadFileFailure;
    long filesize;
    if (!readlong(sock, &filesize))
        return kReadFileFailure;
    if (filesize > 0)
    {
        char buffer[1024];
        do
        {
            int num = std::min<long int>(filesize, sizeof(buffer));
            if (!readdata(sock, buffer, num))
                return kReadFileFailure;
            int offset = 0;
            do
            {
                size_t written = fwrite(&buffer[offset], 1, num-offset, f);
                if (written < 1)
                    return kReadFileFailure;
                offset += written;
            }
            while (offset < num);
            filesize -= num;
        }
        while (filesize > 0);
    }
    return partition_id;
}

bool ReceiveDBFile(int sockfd)
{
	std::string tmp_file = "db_transfer.tmp";
	FILE *fh = fopen(tmp_file.c_str(), "wb");
	long partition_id_long = readfile(sockfd, fh);
	fclose(fh);
	if (partition_id_long == kReadFileFailure)
	{
		return false;
	}

	partition_t partition_id = (partition_t) partition_id_long;
	std::string partition_filename = GetPartitionDBFilename(partition_id);
	
    g_current_node_state->partitions_owned_map_lock.acquireWRLock(); // 1
	assert(g_current_node_state->partitions_owned_map.find(partition_id) 
		!= g_current_node_state->partitions_owned_map.end()); // we're actually supposed to receive this partition
	
	PartitionMetadata pm = g_current_node_state->partitions_owned_map[partition_id];
	if (pm.state == PartitionState::RECEIVING)
	{
		rename(tmp_file.c_str(), partition_filename.c_str());
		pm.db = new PartitionDB(partition_filename);
		pm.state = PartitionState::RECEIVED;
		g_current_node_state->partitions_owned_map[partition_id] = pm;
		g_current_node_state->savePartitionState(g_owned_partition_state_filename);
	}
	g_current_node_state->partitions_owned_map_lock.releaseWRLock(); // 1

	return true;
}

bool SendDBFile(int sockfd, std::string &hostname, std::string &filename, partition_t partition_id)
{
	long partition_id_long = partition_id;
	FILE *fh = fopen(filename.c_str(), "rb");
	if (!fh)
	{
		throw "could not open db file for transfer";
	}
    int sockfd = createClientSocket(hostname, TODO!!!!! PORT); <------------------------------------------------
	return sendfile(sockfd, partition_id, fh);
}