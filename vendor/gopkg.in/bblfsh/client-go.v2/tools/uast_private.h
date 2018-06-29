#ifndef LIBUAST_UAST_PRIVATE_H_
#define LIBUAST_UAST_PRIVATE_H_

#include <libxml/tree.h>

#include "uast.h"

// These functions are used internally for testing and not exported.

// Sets the size of nodes, allocating space if needed.
// Returns 0 if the size was changed correctly.
int NodesSetSize(Nodes *nodes, int len);

// Returns the actual capacity of nodes.
int NodesCap(const Nodes *nodes);

// Returns the node_iface used by Uast
NodeIface UastGetIface(const Uast *ctx);

#endif  // LIBUAST_UAST_PRIVATE_H_
