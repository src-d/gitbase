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

extern char* goGetInternalType(Uast*, NodeHandle);
extern char* goGetToken(Uast*, NodeHandle);
extern int goGetChildrenSize(Uast*, NodeHandle);
extern NodeHandle goGetChild(Uast*, NodeHandle, int);
extern int goGetRolesSize(Uast*, NodeHandle);
extern uint16_t goGetRole(Uast*, NodeHandle, int);
extern int goGetPropertiesSize(Uast*, NodeHandle);
extern char* goGetPropertyKey(Uast*, NodeHandle, int);
extern char* goGetPropertyValue(Uast*, NodeHandle, int);
extern bool goHasStartOffset(Uast*, NodeHandle);
extern uint32_t goGetStartOffset(Uast*, NodeHandle);
extern bool goHasStartLine(Uast*, NodeHandle);
extern uint32_t goGetStartLine(Uast*, NodeHandle);
extern bool goHasStartCol(Uast*, NodeHandle);
extern uint32_t goGetStartCol(Uast*, NodeHandle);
extern bool goHasEndOffset(Uast*, NodeHandle);
extern uint32_t goGetEndOffset(Uast*, NodeHandle);
extern bool goHasEndLine(Uast*, NodeHandle);
extern uint32_t goGetEndLine(Uast*, NodeHandle);
extern bool goHasEndCol(Uast*, NodeHandle);
extern uint32_t goGetEndCol(Uast*, NodeHandle);

static const char *InternalType(const Uast* ctx, NodeHandle node) {
  return goGetInternalType((Uast*)ctx, node);
}

static const char *Token(const Uast* ctx, NodeHandle node) {
  return goGetToken((Uast*)ctx, node);
}

static size_t ChildrenSize(const Uast* ctx, NodeHandle node) {
  return goGetChildrenSize((Uast*)ctx, node);
}

static NodeHandle ChildAt(const Uast* ctx, NodeHandle data, int index) {
  return goGetChild((Uast*)ctx, data, index);
}

static size_t RolesSize(const Uast* ctx, NodeHandle node) {
  return goGetRolesSize((Uast*)ctx, node);
}

static uint16_t RoleAt(const Uast* ctx, NodeHandle node, int index) {
  return goGetRole((Uast*)ctx, node, index);
}

static size_t PropertiesSize(const Uast* ctx, NodeHandle node) {
  return goGetPropertiesSize((Uast*)ctx, node);
}

static const char *PropertyKeyAt(const Uast* ctx, NodeHandle node, int index) {
  return goGetPropertyKey((Uast*)ctx, node, index);
}

static const char *PropertyValueAt(const Uast* ctx, NodeHandle node, int index) {
  return goGetPropertyValue((Uast*)ctx, node, index);
}

static bool HasStartOffset(const Uast* ctx, NodeHandle node) {
  return goHasStartOffset((Uast*)ctx, node);
}

static uint32_t StartOffset(const Uast* ctx, NodeHandle node) {
  return goGetStartOffset((Uast*)ctx, node);
}

static bool HasStartLine(const Uast* ctx, NodeHandle node) {
  return goHasStartLine((Uast*)ctx, node);
}

static uint32_t StartLine(const Uast* ctx, NodeHandle node) {
  return goGetStartLine((Uast*)ctx, node);
}

static bool HasStartCol(const Uast* ctx, NodeHandle node) {
  return goHasStartCol((Uast*)ctx, node);
}

static uint32_t StartCol(const Uast* ctx, NodeHandle node) {
  return goGetStartCol((Uast*)ctx, node);
}

static bool HasEndOffset(const Uast* ctx, NodeHandle node) {
  return goHasEndOffset((Uast*)ctx, node);
}

static uint32_t EndOffset(const Uast* ctx, NodeHandle node) {
  return goGetEndOffset((Uast*)ctx, node);
}

static bool HasEndLine(const Uast* ctx, NodeHandle node) {
  return goHasEndLine((Uast*)ctx, node);
}

static uint32_t EndLine(const Uast* ctx, NodeHandle node) {
  return goGetEndLine((Uast*)ctx, node);
}

static bool HasEndCol(const Uast* ctx, NodeHandle node) {
  return goHasEndCol((Uast*)ctx, node);
}

static uint32_t EndCol(const Uast* ctx, NodeHandle node) {
  return goGetEndCol((Uast*)ctx, node);
}

static Uast* CreateUast() {
  return UastNew((NodeIface){
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

#endif // CLIENT_GO_BINDINGS_H_
