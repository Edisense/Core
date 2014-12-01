#ifndef BLE_CLIENT_INTERNAL_H
#define BLE_CLIENT_INTERNAL_H

#include "edisense_types.h"

bool Put(device_t device_id, time_t timestamp, time_t expiration, void *data, size_t data_len);

#endif /* BLE_CLIENT_INTERNAL_H */