
#include <cassert>
#include <cstring>
#include <map>
#include <mutex>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <climits>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <edisense_types.h>
#include <server/server-internal.h>

#include "command.h"
#include "state.h"

#include "util/socket-util.h"

using namespace std;

#define NOT_IMPLEMENTED printf("NOT_IMPLEMENTED\n"); exit(0);

//map<string, map<time_t, string>> data;
//mutex data_lock;

// Server constants to service internal RPCs
static const unsigned short INTERNAL_SERVER_PORT_NO = 3000;
static const int MAX_INTERNAL_SERVER_BACKLOG = 100;

// Server constants to service front end requests
static const unsigned short FRONT_END_SERVER_PORT_NO = 4000;
static const int MAX_FRONT_END_SERVER_BACKLOG = 15;

// Node id computed from hash of hostname
uint32_t node_id;

// Counter for transaction ids
static uint64_t transaction_count;
static mutex transaction_count_lock; 

static uint32_t getNodeId(const string &hostname)
{
  hash<string> h;
  return (uint32_t) h(hostname);
}

static uint64_t incrementTransactionCount()
{
  uint64_t tid;
  lock_guard<mutex> lock(transaction_count_lock);
  tid = transaction_count++;
//  if (tid % SOMENUMBER) checkpoint tid
  return tid;
}

// Process internal client requests
static void handleInternalClientRequest(int client_fd)
{
  char command[COMMAND_ID_LEN];
  int bytesRead = read(client_fd, command, sizeof(command));
  if (bytesRead != sizeof(command) || command[COMMAND_ID_LEN-1] != '\0')
  {
    perror("Invalid command received.");
    close(client_fd);
    return;
  }
  cout << "Received command: " << command << endl;
  
  if (strcmp(command, PUT) == 0)
  {
    handleInternalPutRequest(client_fd);
  }
  else if (strcmp(command, GET) == 0)
  {
    handleInternalGetRequest(client_fd);
  }
  else if (strcmp(command, RECOVER) == 0)
  {
    handleInternalRecoverRequest(client_fd);
  }
  else if (strcmp(command, MIGRATE_RANGE) == 0)
  {
    handleInternalMigrateRangeRequest(client_fd);
  }
  else if (strcmp (command, UPDATE_RANGE) == 0)
  {
    handleInternalUpdateRangeRequest(client_fd);
  }
  else if (strcmp (command, LEAVE) == 0)
  {
    handleInternalLeaveRequest(client_fd);
  }
  else if (strcmp (command, JOIN) == 0)
  {
    handleInternalJoinRequest(client_fd);
  }
  else
  {
    cout << "unrecognized message " << command << endl;
  }

  close(client_fd);
}

static void internalServer()
{
  int server_fd = createServerSocket(INTERNAL_SERVER_PORT_NO, MAX_INTERNAL_SERVER_BACKLOG);
  if (server_fd == kServerSocketFailure)
  {
    perror("Unable to start front end server. Exiting.");
    exit(1);
  }

  struct sockaddr_in cli_addr;
  socklen_t cli_len = sizeof(cli_addr);
//  while (true)
//  {
//  	int client_fd = accept(server_fd, (struct sockaddr *) &cli_addr, &cli_len);
//  	if (client_fd < 0)
//  	{
//  		perror("Error on accept");
//  		close(client_fd);
//  		continue;
//  	}
//
//  	thread client_thread(handleInternalClientRequest(client_fd));
//    client_thread.detach();
//	}

	close(server_fd);
}

static void frontEndServer()
{
  int server_fd = createServerSocket(FRONT_END_SERVER_PORT_NO, MAX_FRONT_END_SERVER_BACKLOG);
  if (server_fd == kServerSocketFailure)
  {
    perror("Unable to start front end server. Exiting.");
    exit(1);
  }

  struct sockaddr_in cli_addr;
  socklen_t cli_len = sizeof(cli_addr);
  while (true)
  {
    int client_fd = accept(server_fd, (struct sockaddr *) &cli_addr, &cli_len);
    if (client_fd < 0)
    {
      perror("Error on accept");
      close(client_fd);
      continue;
    }

    NOT_IMPLEMENTED
  }

  close(server_fd);
}

static void initializeState()
{
  char hostname[HOST_NAME_MAX + 1]; 
  if (gethostname(hostname, sizeof(hostname)) != 0)
  {
    perror("Unable to gethostname for current machine. Exiting.");
    exit(1);
  }
  node_id = getNodeId(string(hostname));
  transaction_count = 0;
}

int main(int argc, const char *argv[])
{
	thread internal_comms_server_thread(internalServer);
  thread front_end_server_thread(frontEndServer);

  internal_comms_server_thread.join();
  front_end_server_thread.join();
}