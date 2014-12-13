Core
====

Runtime executable for Edisense

To start a node on the cluster, pass these parameters to the EdisenseCore final executable:
* `--datadir` - The name of a directory to store the data in.  *NOTE* This directory should not exist when the cluster is started, or the system will exit to prevent overwriting previous data.
* `--nodestate` - A filename for storing the current node state (used for recovery)
* `--clustermembers` - A newline separated list of hostnames pointing to other members of the cluster
* `--ownershipmap` - A filename for storing the list of partitions replicated by this node
* `--partitionmap` - A file contiaining the initial partition configuration.  This file is generated from the cluster mapping by the PartitionGenerator tool
* `--join` - An optional flag indicating that this node should join an existing cluster
* `--recover` - An optional flag indicating that this node should be started in recovery mode
* `--debug` - An optional flag indicating that this node should provide simulated data to the cluster
 
To generate the partition mapping, pass the same clustermembers file to the PartitionGenerator:

`~ $ PartitionGenerator <clustermembers file> <output filename> <number of desired partitions> <replication factor>`

The cluster will begin passing data to the other cluster members as soon as the target recipient is also active and listening

[![Build Status](https://travis-ci.org/Edisense/Core.svg)](https://travis-ci.org/Edisense/Core)
