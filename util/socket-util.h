
#pragma once

#include <string>

/**
 * Constant: kServerSocketFailure
 * ------------------------------
 * Constant returned by createServerSocket if the
 * server socket couldn't be created or otherwise
 * bound to listen to the specified port.
 */

const int kServerSocketFailure = -1;

/**
 * Function: createServerSocket
 * ----------------------------
 * createServerSocket creates a server socket to
 * listen for all client connections on the given
 * port.  The function returns a valid server socket
 * descriptor that allows a specified backlog of
 * connection requests to accummulate before
 * additional requests might be refused.
 */

int createServerSocket(unsigned short port, int backlog);

/**
 * Constant: kClientSocketError
 * ----------------------------
 * Sentinel used to communicate that a connection
 * to a remote server could not be made.
 */

const int kClientSocketError = -1;

/**
 * Function: createClientSocket
 * ----------------------------
 * Establishes a connection to the provided host
 * on the specified port, and returns a bidirectional 
 * socket descriptor that can be used for two-way
 * communication with the service running on the
 * identified host's port.
 */

int createClientSocket(const std::string& host, 
		       unsigned short port);