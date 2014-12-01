#ifndef DAEMONS_H
#define DAEMONS_H

// Background thread for initiating donation requests
void LoadBalanceDaemon(unsigned int freq);

// Background thread for garbage collecting data
void GarbageCollectDaemon(unsigned int freq);

#endif /* DAEMONS_H */