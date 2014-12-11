#ifndef DAEMONS_H
#define DAEMONS_H

// Background thread for initiating donation requests
void LoadBalanceDaemon(edisense_comms::Member* member, unsigned int freq, std::string logfile);

// Background thread for garbage collecting data
void GarbageCollectDaemon(unsigned int freq);

// Background thread for retrying failed puts
void RetryPutDaemon(unsigned int freq);

// Separate daemon for receiving db shards
void DBTransferServerDaemon();

#endif /* DAEMONS_H */