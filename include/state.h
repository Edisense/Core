#ifndef STATE_H
#define STATE_H

#include <stdint.h>

enum NODE_STATE 
{ 
	JOINING,
	LEAVING,
	STABLE,
	RECOVERING
};

extern NODE_STATE state;

extern uint32_t node_id;

extern bool donating;
extern bool receiving;

#endif /* STATE_H */