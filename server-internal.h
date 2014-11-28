#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

void handleInternalPutRequest(int client_fd);

void handleInternalGetRequest(int client_fd);

void handleInternalRecoverRequest(int client_fd);

void handleInternalMigrateRangeRequest(int client_fd);

void handleInternalUpdateRangeRequest(int client_fd);

void handleInternalLeaveRequest(int client_fd);

void handleInternalJoinRequest(int client_fd);

#endif /* SERVER_INTERNAL_H */