
#ifndef COMMAND_H
#define COMMAND_H

// Length of command identifiers
#define COMMAND_ID_LEN 4

/* Put
 * ---
 * 4 b for node id
 * 8 b for transaction id
 * 2 b for device id
 * 4 b for timestamp
 * 4 b for expiration time
 * 2 b for size of payload
 */
#define PUT "PUT"
#define PUT_HEADER_SIZE 24

/* Get
 * ---
 * 4 b for node id
 * 8 b for transaction id
 * 2 b for device_id
 * 4 b for lower time bound
 * 4 b for upper time bound
 */
#define GET "GET"
#define GET_HEADER_SIZE 22

/* Recover
 * -------
 * 4 b for node id
 * 8 b for transaction id
 * 4 b for range id
 * 4 b for lower time bound
 */
#define RECOVER "REC"
#define RECOVER_HEADER_SIZE 20

// Donate

/* Migrate range
 * -------------
 * 4 b for node id
 * 8 b for transaction id
 * 4 b for range id
 * 4 b for size estimate
 */
#define MIGRATE_RANGE "DMR"
#define MIGRATE_RANGE_HEADER_SIZE 20

/* Update range
 * ------------
 * 4 b for node id
 * 8 b for transaction id
 * 4 b for range id
 * 2 b for new owner
 */
#define UPDATE_RANGE "DUR"
#define UPDATE_RANGE_HEADER_SIZE 20

/* Leave
 * -----
 * 4 b for node id
 * 8 b for transaction id
 */
#define LEAVE "LEA"
#define LEAVE_HEADER_SIZE 12

/* Join
 * -----
 * 4 b for node id
 * 8 b for transaction id
 */
#define JOIN "JOI"
#define JOIN_HEADER_SIZE 12

#endif /* COMMAND_H */