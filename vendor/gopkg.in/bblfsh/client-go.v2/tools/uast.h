#ifndef LIBUAST_UAST_H_
#define LIBUAST_UAST_H_

#include "export.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Uast stores the general context required for library functions.
// It must be initialized with `UastNew` passing a valid implementation of the
// `NodeIface` interface.
// Once it is not used anymore, it shall be released calling `UastFree`.
typedef struct Uast Uast;

typedef uintptr_t NodeHandle;

// This interface must be implemented to create a Uast context.
typedef struct NodeIface {
  const char *(*InternalType)(const Uast*, NodeHandle);
  const char *(*Token)(const Uast*, NodeHandle);

  // Children
  size_t (*ChildrenSize)(const Uast*, NodeHandle);
  NodeHandle (*ChildAt)(const Uast*, NodeHandle, int);

  // Roles
  size_t (*RolesSize)(const Uast*, NodeHandle);
  uint16_t (*RoleAt)(const Uast*, NodeHandle, int);

  // Properties
  size_t (*PropertiesSize)(const Uast*, NodeHandle);
  const char *(*PropertyKeyAt)(const Uast*, NodeHandle, int);
  const char *(*PropertyValueAt)(const Uast*, NodeHandle, int);

  // Postion
  bool (*HasStartOffset)(const Uast*, NodeHandle);
  uint32_t (*StartOffset)(const Uast*, NodeHandle);
  bool (*HasStartLine)(const Uast*, NodeHandle);
  uint32_t (*StartLine)(const Uast*, NodeHandle);
  bool (*HasStartCol)(const Uast*, NodeHandle);
  uint32_t (*StartCol)(const Uast*, NodeHandle);

  bool (*HasEndOffset)(const Uast*, NodeHandle);
  uint32_t (*EndOffset)(const Uast*, NodeHandle);
  bool (*HasEndLine)(const Uast*, NodeHandle);
  uint32_t (*EndLine)(const Uast*, NodeHandle);
  bool (*HasEndCol)(const Uast*, NodeHandle);
  uint32_t (*EndCol)(const Uast*, NodeHandle);

} NodeIface;

typedef struct Nodes Nodes;

// Returns the amount of nodes
EXPORT int NodesSize(const Nodes *nodes);

// Returns the node at the given index.
EXPORT NodeHandle NodeAt(const Nodes *nodes, int index);

// Releases the resources associated with nodes
EXPORT void NodesFree(Nodes *nodes);

// An UastIterator is used to keep the state of the current iteration over the tree.
// It's initialized with UastIteratorNew, used with UastIteratorNext and freed
// with UastIteratorFree.
typedef struct UastIterator UastIterator;

typedef enum { PRE_ORDER, POST_ORDER, LEVEL_ORDER, POSITION_ORDER } TreeOrder;

// Uast needs a node implementation in order to work. This is needed
// because the data structure of the node itself is not defined by this
// library, instead it provides an interface that is expected to be satisfied by
// the binding providers.
//
// This architecture allows libuast to work with every language's native node
// data structures.
//
// Returns NULL and sets LastError if the Uast couldn't initialize.
EXPORT Uast *UastNew(NodeIface iface);

// Releases Uast resources.
EXPORT void UastFree(Uast *ctx);

// Returns the list of native root nodes that satisfy the xpath query,
// or NULL if there was any error.
//
// An XPath Query must follow the XML Path Language (XPath) Version 1 spec.
// For further information about xpath and its syntax checkout:
// https://www.w3.org/TR/xpath/
//
// A node will be mapped to the following XML representation:
// ```
// <{{INTERNAL_TYPE}} token={{TOKEN}} role{{ROLE[n]}} prop{{PROP[n]}}>
//   ... children
// </{{INTERNAL_TYPE}}>
// ```
//
// An example in Python:
// ```
// <NumLiteral token="2" roleLiteral roleSimpleIdentifier></NumLiteral>
// ```
//
// It will return an error if the query has a return type that is not a
// node list. In that case, you should use one of the typed filter functions
// (`UastFilterBool`, `UastFilterNumber` or `UastFilterString`).
EXPORT Nodes *UastFilter(const Uast *ctx, NodeHandle node, const char *query);

// Returns a integer value as result of executing the XPath query with bool result,
// with `1` meaning `true` and `0` false. If there is any error, the flag `ok` will
// be set to false. The parameters have the same meaning as `UastFilter`.
EXPORT bool UastFilterBool(const Uast *ctx, NodeHandle node, const char *query, bool *ok);

// Returns a `double` value as result of executing the XPath query with number result.
// The parameters have the same meaning as `UastFilter`. If there is any error,
// the flag `ok` will be set to false.
EXPORT double UastFilterNumber(const Uast *ctx, NodeHandle node, const char *query, bool *ok);

// Returns a `const char*` value as result of executing the XPath query with
// a string result. The parameters have the same meaning as `UastFilter`. The user
// takes ownership of the returned `const char *` and thus must free it.
// If there is any error, the return value will be `NULL`.
EXPORT const char *UastFilterString(const Uast *ctx, NodeHandle node, const char *query);

// Create a new UastIterator pointer. This will allow you to traverse the UAST
// calling UastIteratorNext. The node argument will be user as the root node of
// the iteration. The TreeOrder argument specifies the traversal mode. It can be
// PRE_ORDER, POST_ORDER or LEVEL_ORDER. Once you've used the UastIterator, it must
// be frees using UastIteratorFree.
//
// Returns NULL and sets LastError if the UastIterator couldn't initialize.
EXPORT UastIterator *UastIteratorNew(const Uast *ctx, NodeHandle node, TreeOrder order);

// Same as UastIteratorNew, but also allows to specify a transform function taking a node
// and returning it. This is specially useful when the bindings need to do operations like
// increasing / decreasing the language reference count when new nodes are added to the
// iterator internal data structures.
UastIterator *UastIteratorNewWithTransformer(const Uast *ctx, NodeHandle node,
                                             TreeOrder order, NodeHandle(*transform)(NodeHandle));

// Frees a UastIterator.
EXPORT void UastIteratorFree(UastIterator *iter);

// Retrieve the next node of the traversal of an UAST tree or NULL if the
// traversal has finished.
EXPORT NodeHandle UastIteratorNext(UastIterator *iter);

// Returns a string with the latest error.
// It may be an empty string if there's been no error.
//
// Memory for the string is obtained with malloc, and can be freed with free.
EXPORT char *LastError(void);

#ifdef __cplusplus
}  // extern "C"
#endif
#endif  // LIBUAST_UAST_H_
