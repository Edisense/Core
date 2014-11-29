#ifndef PARTITION_IO_H
#define PARTITION_IO_H

#include <fcntl.h>
#include <unistd.h>

#include <edisense_types.h>

// read binary encoded partition table: PARTITION REPLICAS TABLE
partition_t *readPartitionTable(const char *filename, 
	int *n_partitions, int *n_replicas)
{
	int fd = open(filename, O_RDONLY);
	if (!fd) 
		return NULL;
	uint32_t partitions, replicas;
	if (read(fd, &partitions, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	if (read(fd, &replicas, sizeof(uint32_t)) != sizeof(uint32_t)) 
		return NULL;
	size_t table_size = sizeof(partition_t) * partitions * replicas;
	partition_t *table = (partition_t *) malloc(table_size);
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
	int fd = open(filename, O_WRONLY | O_TRUNC);
	if (fd == 0) 
	{
		return false;
	}
	uint32_t buf = n_partitions;
	if (write(fd, &buf, sizeof(uint32_t)) != sizeof(uint32_t)) 
	{
		close(fd);
		return false;
	}
	buf = n_replicas;
	if (write(fd, &buf, sizeof(uint32_t)) != sizeof(uint32_t)) 
	{
		close(fd);
		return false;
	}
	size_t table_size = sizeof(partition_t) * n_partitions *n_replicas;
	if (write(fd, table, table_size) != table_size) 
	{
		close(fd);
		return false;
	}
	if (fsync(fd) != 0) 
	{
		close(fd);
		return false;
	}
	close(fd);
	return true;
}

#endif /* PARTITION_IO_H */