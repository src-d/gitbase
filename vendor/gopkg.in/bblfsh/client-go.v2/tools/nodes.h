#ifndef LIBUAST_NODES_H_
#define LIBUAST_NODES_H_

#include "export.h"

typedef struct Nodes Nodes;

// Returns the amount of nodes
EXPORT int NodesSize(const Nodes *nodes);

// Returns the node at the given index.
EXPORT void *NodeAt(const Nodes *nodes, int index);

// Releases the resources associated with nodes
EXPORT void NodesFree(Nodes *nodes);

#endif  // LIBUAST_NODES_H_
