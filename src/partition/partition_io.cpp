
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cassert>

#include "partition_io.h"

// read binary encoded partition table: PARTITION REPLICAS TABLE
node_t *readPartitionTable(const char *filename, 
	int *n_partitions, int *n_replicas)
{
	int fd = open(filename, O_RDONLY);
	if (fd == -1) 
		return NULL;
	uint32_t partitions, replicas;
	if (read(fd, &partitions, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	if (read(fd, &replicas, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	size_t table_size = sizeof(node_t) * partitions * replicas;
	node_t *table = (node_t *) malloc(table_size);
	size_t bytes_read = read(fd, table, table_size);
	if (bytes_read != table_size)
	{
		free(table);
		return NULL;
	}
	*n_partitions = partitions;
	*n_replicas = replicas;
	close(fd);
	return table;
}

bool writePartitionTable(const char *filename, partition_t *table,
	int n_partitions, int n_replicas)
{
	int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
	if (fd < 0) 
	{
		perror("could not open partition map file.");
		return false;
	}
	uint32_t buf = n_partitions;
	if (write(fd, &buf, sizeof(uint32_t)) != sizeof(uint32_t)) 
	{
		perror("could not write n_partitions.");
		close(fd);
		return false;
	}
	buf = n_replicas;
	if (write(fd, &buf, sizeof(uint32_t)) != sizeof(uint32_t)) 
	{
		perror("could not write n_replicas.");
		close(fd);
		return false;
	}
	size_t table_size = sizeof(partition_t) * n_partitions *n_replicas;
	if (write(fd, table, table_size) != table_size) 
	{
		perror("could not write table.");
		close(fd);
		return false;
	}
	if (fsync(fd) != 0) 
	{
		perror("could fsync file.");
		close(fd);
		return false;
	}
	close(fd);
	return true;
}
