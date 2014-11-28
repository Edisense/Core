#ifndef EDISENSE_TYPES_H
#define EDISENSE_TYPES_H

#include <cstdint>

//TODO Choose appropriate length
#define HOST_NAME_MAX 40

// 16-bit node id
typedef uint16_t node_t;

// 16-bit sensor id
typedef uint16_t device_t;

// 16-bit range id
typedef uint16_t partition_t;

#endif /* EDISENSE_TYPES_H */