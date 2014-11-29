
#include "hash.h"

// source: http://www.cse.yorku.ca/~oz/hash.html
inline unsigned int hash_string(const char *str);
{
	unsigned int hash = 5381;
    int c;
    while (c = *str++)
    	hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
	return hash;
}

// source: java random number generator
inline unsigned int hash_integer(int n)
{
	return n * (unsigned long long)25214903917 + 11;
}

inline node_t hostToNodeID(std::string &host)
{
	return (node_t) hash_string(host.c_str());
}
