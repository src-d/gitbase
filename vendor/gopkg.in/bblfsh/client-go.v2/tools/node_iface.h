#ifndef LIBUAST_NODE_IFACE_H_
#define LIBUAST_NODE_IFACE_H_

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

// This interface must be implemented to create a Uast context.
typedef struct NodeIface {
  const char *(*InternalType)(const void *);
  const char *(*Token)(const void *);

  // Children
  size_t (*ChildrenSize)(const void *);
  void *(*ChildAt)(const void *, int);

  // Roles
  size_t (*RolesSize)(const void *);
  uint16_t (*RoleAt)(const void *, int);

  // Properties
  size_t (*PropertiesSize)(const void *);
  const char *(*PropertyKeyAt)(const void *, int);
  const char *(*PropertyValueAt)(const void *, int);

  // Postion
  bool (*HasStartOffset)(const void *);
  uint32_t (*StartOffset)(const void *);
  bool (*HasStartLine)(const void *);
  uint32_t (*StartLine)(const void *);
  bool (*HasStartCol)(const void *);
  uint32_t (*StartCol)(const void *);

  bool (*HasEndOffset)(const void *);
  uint32_t (*EndOffset)(const void *);
  bool (*HasEndLine)(const void *);
  uint32_t (*EndLine)(const void *);
  bool (*HasEndCol)(const void *);
  uint32_t (*EndCol)(const void *);

} NodeIface;

#endif  // LIBUAST_NODE_IFACE_H_
