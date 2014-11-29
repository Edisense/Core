#ifndef PARTITION_IO_H
#define PARTITION_IO_H

#include <edisense_types.h>

partition_t *readPartitionTable(const char *filename, 
	int *n_partitions, int *n_replicas);

bool writePartitionTable(const char *filename, partition_t *table,
	int n_partitions, int n_replicas);

#endif /* PARTITION_IO_H */