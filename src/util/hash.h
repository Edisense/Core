#ifndef HASH_FUNCTION_H
#define HASH_FUNCTION_H

#include <string>

#include "edisense_types.h"

// hash functions

// source: http://www.cse.yorku.ca/~oz/hash.html
static inline unsigned int hash_string(const char *str)
{
  unsigned int hash = 5381;
  int c;
  while ((c = *str++))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  return hash;
}

// source: java random number generator
static inline unsigned int hash_integer(int n)
{
  return n * (unsigned long long)25214903917 + 11;
}

// Convenience wrapper

static inline node_t hostToNodeId(const std::string &host)
{
  return (node_t) hash_string(host.c_str());
}


#endif /* HASH_FUNCTION_H */