#ifndef HASH_FUNCTION_H
#define HASH_FUNCTION_H

#include <string>

#include "edisense_types.h"

// hash functions

inline unsigned int hash_string(const char *);

inline unsigned int hash_integer(int);

// convenience wrapper

inline node_t hostToNodeId(std::string&);

#endif /* HASH_FUNCTION_H */