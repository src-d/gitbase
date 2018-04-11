#ifndef CLIENT_GO_BINDINGS_H_
#define CLIENT_GO_BINDINGS_H_

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#if __has_include("uast.h") // std C++17, GCC 5.x || Clang || VSC++ 2015u2+
// Embedded mode on UNIX, MSVC build on Windows.
#include "uast.h"
#else
// Hosted mode on UNIX, MinGW build on Windows.
#include "libuast/uast.h"
#endif

extern char* goGetInternalType(uintptr_t);
extern char* goGetToken(uintptr_t);
extern int goGetChildrenSize(uintptr_t);
extern uintptr_t goGetChild(uintptr_t, int);
extern int goGetRolesSize(uintptr_t);
extern uint16_t goGetRole(uintptr_t, int);
extern int goGetPropertiesSize(uintptr_t);
extern char* goGetPropertyKey(uintptr_t, int);
extern char* goGetPropertyValue(uintptr_t, int);
extern bool goHasStartOffset(uintptr_t);
extern uint32_t goGetStartOffset(uintptr_t);
extern bool goHasStartLine(uintptr_t);
extern uint32_t goGetStartLine(uintptr_t);
extern bool goHasStartCol(uintptr_t);
extern uint32_t goGetStartCol(uintptr_t);
extern bool goHasEndOffset(uintptr_t);
extern uint32_t goGetEndOffset(uintptr_t);
extern bool goHasEndLine(uintptr_t);
extern uint32_t goGetEndLine(uintptr_t);
extern bool goHasEndCol(uintptr_t);
extern uint32_t goGetEndCol(uintptr_t);

static const char *InternalType(const void *node) {
  return goGetInternalType((uintptr_t)node);
}

static const char *Token(const void *node) {
  return goGetToken((uintptr_t)node);
}

static size_t ChildrenSize(const void *node) {
  return goGetChildrenSize((uintptr_t)node);
}

static void *ChildAt(const void *data, int index) {
  return (void*)goGetChild((uintptr_t)data, index);
}

static size_t RolesSize(const void *node) {
  return goGetRolesSize((uintptr_t)node);
}

static uint16_t RoleAt(const void *node, int index) {
  return goGetRole((uintptr_t)node, index);
}

static size_t PropertiesSize(const void *node) {
  return goGetPropertiesSize((uintptr_t)node);
}

static const char *PropertyKeyAt(const void *node, int index) {
  return goGetPropertyKey((uintptr_t)node, index);
}

static const char *PropertyValueAt(const void *node, int index) {
  return goGetPropertyValue((uintptr_t)node, index);
}

static bool HasStartOffset(const void *node) {
  return goHasStartOffset((uintptr_t)node);
}

static uint32_t StartOffset(const void *node) {
  return goGetStartOffset((uintptr_t)node);
}

static bool HasStartLine(const void *node) {
  return goHasStartLine((uintptr_t)node);
}

static uint32_t StartLine(const void *node) {
  return goGetStartLine((uintptr_t)node);
}

static bool HasStartCol(const void *node) {
  return goHasStartCol((uintptr_t)node);
}

static uint32_t StartCol(const void *node) {
  return goGetStartCol((uintptr_t)node);
}

static bool HasEndOffset(const void *node) {
  return goHasEndOffset((uintptr_t)node);
}

static uint32_t EndOffset(const void *node) {
  return goGetEndOffset((uintptr_t)node);
}

static bool HasEndLine(const void *node) {
  return goHasEndLine((uintptr_t)node);
}

static uint32_t EndLine(const void *node) {
  return goGetEndLine((uintptr_t)node);
}

static bool HasEndCol(const void *node) {
  return goHasEndCol((uintptr_t)node);
}

static uint32_t EndCol(const void *node) {
  return goGetEndCol((uintptr_t)node);
}

static Uast *ctx;
static Nodes *nodes;

static void CreateUast() {
  ctx = UastNew((NodeIface){
      .InternalType = InternalType,
      .Token = Token,
      .ChildrenSize = ChildrenSize,
      .ChildAt = ChildAt,
      .RolesSize = RolesSize,
      .RoleAt = RoleAt,
      .PropertiesSize = PropertiesSize,
      .PropertyKeyAt = PropertyKeyAt,
      .PropertyValueAt = PropertyValueAt,
      .HasStartOffset = HasStartOffset,
      .StartOffset = StartOffset,
      .HasStartLine = HasStartLine,
      .StartLine = StartLine,
      .HasStartCol = HasStartCol,
      .StartCol = StartCol,
      .HasEndOffset = HasEndOffset,
      .EndOffset = EndOffset,
      .HasEndLine = HasEndLine,
      .EndLine = EndLine,
      .HasEndCol = HasEndCol,
      .EndCol = EndCol,
  });
}

static bool Filter(uintptr_t node_ptr, const char *query) {
  nodes = UastFilter(ctx, (void*)node_ptr, query);
  return nodes != NULL;
}

static int FilterBool(uintptr_t node_ptr, const char *query) {
  bool ok;
  bool res = UastFilterBool(ctx, (void*)node_ptr, query, &ok);
  if (!ok) {
    return -1;
  }
  return (int)res;
}

static double FilterNumber(uintptr_t node_ptr, const char *query, int *ok) {
  bool c_ok;
  double res = UastFilterNumber(ctx, (void*)node_ptr, query, &c_ok);
  if (!c_ok) {
    *ok = 0;
  } else {
    *ok = 1;
  }
  return res;
}

static const char *FilterString(uintptr_t node_ptr, const char *query) {
  return UastFilterString(ctx, (void*)node_ptr, query);
}

static uintptr_t IteratorNew(uintptr_t node_ptr, int order) {
  return (uintptr_t)UastIteratorNew(ctx, (void *)node_ptr, order);
}

static uintptr_t IteratorNext(uintptr_t iter) {
  return (uintptr_t)UastIteratorNext((void*)iter);
}

static void IteratorFree(uintptr_t iter) {
  UastIteratorFree((void*)iter);
}

static char *Error() {
  return LastError();
}

static int Size() {
  return NodesSize(nodes);
}

static uintptr_t At(int i) {
  return (uintptr_t)NodeAt(nodes, i);
}

#endif // CLIENT_GO_BINDINGS_H_
