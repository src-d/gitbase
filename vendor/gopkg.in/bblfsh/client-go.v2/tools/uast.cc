#include "roles.h"
#include "testing_tools.h"
#include "uast.h"
#include "uast_private.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cstdbool>
#include <cstring>
#include <deque>
#include <memory>
#include <new>
#include <set>
#include <vector>

#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#define _CRT_NONSTDC_NO_DEPRECATE

#define BUF_SIZE 256
char error_message[BUF_SIZE];

struct Uast {
  NodeIface iface;
};

struct UastIterator {
  const Uast *ctx;
  TreeOrder order;
  std::deque<NodeHandle> pending;
  std::set<NodeHandle> visited;
  NodeHandle (*nodeTransform)(NodeHandle);
  bool preloaded;
};

struct Nodes {
  std::vector<NodeHandle> results;
  int len;
  int cap;
};

const std::vector<const char *> Type2Str = {
  "UNDEFINED",
  "NODESET",
  "BOOLEAN",
  "NUMBER",
  "STRING",
  "POINT",
  "RANGE",
  "LOCATIONSET",
  "USERS",
  "XSLT_TREE"
};

static xmlDocPtr CreateDocument(const Uast *ctx, NodeHandle node);
static xmlNodePtr CreateXmlNode(const Uast *ctx, NodeHandle node, xmlNodePtr parent);
void Error(void *ctx, const char *msg, ...);
// Adds the children of the node to the iterator queue and returns
// if the node was already checked, which will happen with leaf nodes
// or nodes which childs already processed. Used for the POST_ORDER
// iterative traversal algorithm.
static bool Visited(UastIterator *iter, NodeHandle node);
// Get the next element in pre-order traversal mode.
static NodeHandle PreOrderNext(UastIterator *iter);
// Get the next element in level-order traversal mode.
static NodeHandle LevelOrderNext(UastIterator *iter);
// Get the next element in post-order traversal mode.
static NodeHandle PostOrderNext(UastIterator *iter);
// Get the next element in position-order traversal mode.
static NodeHandle PositionOrderNext(UastIterator *iter);

class QueryResult {
  xmlXPathContextPtr xpathCtx;
  xmlDocPtr doc;

  public:
  xmlXPathObjectPtr xpathObj;

  QueryResult(const Uast *ctx, NodeHandle node, const char *query,
              xmlXPathObjectType expected) {

    assert(ctx);
    assert(node);
    assert(query);

    auto handler = (xmlGenericErrorFunc)Error;
    initGenericErrorDefaultFunc(&handler);

    doc = CreateDocument(ctx, node);
    if (!doc) {
      xmlFreeDoc(doc);
      throw std::runtime_error("");
    }

    xpathCtx = static_cast<xmlXPathContextPtr>(xmlXPathNewContext(doc));
    if (!xpathCtx) {
      xmlXPathFreeContext(xpathCtx);
      xmlFreeDoc(doc);
      throw std::runtime_error("");
    }

    xpathObj = xmlXPathEvalExpression(BAD_CAST(query), xpathCtx);
    if (!xpathObj) {
      xmlXPathFreeObject(xpathObj);
      xmlXPathFreeContext(xpathCtx);
      xmlFreeDoc(doc);
      throw std::runtime_error("");
    }

    if (xpathObj->type != expected) {
      Error(nullptr, "Result of expression is not %s (is: %s)\n",
            Type2Str[expected], Type2Str[xpathObj->type]);
      throw std::runtime_error("");
    }
  }

  ~QueryResult()
  {
    if (xpathObj) xmlXPathFreeObject(xpathObj);
    if (xpathCtx) xmlXPathFreeContext(xpathCtx);
    if (doc) xmlFreeDoc(doc);
  }
};


class CreateXMLNodeException: public std::runtime_error {
  public:
  explicit CreateXMLNodeException(const char *msg): runtime_error(msg) {
    Error(nullptr, msg);
  }
  // Keeps LastError
  CreateXMLNodeException(): std::runtime_error("") {}
};

static UastIterator *UastIteratorNewBase(const Uast *ctx, NodeHandle node, TreeOrder order) {
  assert(ctx);
  assert(node);

  UastIterator *iter;

  try {
    iter = new UastIterator();
  } catch (const std::bad_alloc&) {
    Error(nullptr, "Unable to get memory\n");
    return nullptr;
  }

  iter->ctx = ctx;
  iter->order = order;
  iter->preloaded = false;
  return iter;
}

//////////////////////////////
///////// PUBLIC API /////////
//////////////////////////////

void NodesFree(Nodes *nodes) {
  if (nodes != nullptr) {
    delete nodes;
    nodes = nullptr;
  }
}

int NodesSize(const Nodes *nodes) {
  assert(nodes);

  return nodes->len;
}

NodeHandle NodeAt(const Nodes *nodes, int index) {
  assert(nodes);

  if (index < nodes->len) {
    return nodes->results[index];
  }
  return 0;
}

Uast *UastNew(NodeIface iface) {
  Uast *ctx;

  try {
    ctx = new Uast();
  } catch (const std::bad_alloc&) {
    Error(nullptr, "Unable to get memory\n");
    return nullptr;
  }

  if (!ctx) {
    Error(nullptr, "Unable to get memory\n");
    return nullptr;
  }
  xmlInitParser();
  ctx->iface = iface;
  return ctx;
}

void UastFree(Uast *ctx) {
  if (ctx != nullptr) {
    delete ctx;
    ctx = nullptr;
  }

  xmlCleanupParser();
}

UastIterator *UastIteratorNew(const Uast *ctx, NodeHandle node, TreeOrder order) {
  assert(ctx);
  assert(node);

  UastIterator *iter = UastIteratorNewBase(ctx, node, order);
  iter->pending.push_front(node);
  iter->nodeTransform = nullptr;
  return iter;
}

void UastIteratorFree(UastIterator *iter) {
  if (iter != nullptr) {
    delete iter;
    iter = nullptr;
  }
}

UastIterator *UastIteratorNewWithTransformer(const Uast *ctx, NodeHandle node,
                                             TreeOrder order, NodeHandle(*transform)(NodeHandle)) {

  assert(ctx);
  assert(node);
  assert(transform);

  UastIterator *iter = UastIteratorNewBase(ctx, node, order);
  iter->pending.push_front(transform(node));
  iter->nodeTransform = transform;
  return iter;
}

NodeHandle UastIteratorNext(UastIterator *iter) {
  assert(iter);

  if (iter == nullptr || iter->pending.empty()) {
    return 0;
  }

  switch(iter->order) {
    case LEVEL_ORDER:
      return LevelOrderNext(iter);
    case POST_ORDER:
      return PostOrderNext(iter);
    case POSITION_ORDER:
      return PositionOrderNext(iter);
    default:
      return PreOrderNext(iter);
  }
}

NodeIface UastGetIface(const Uast *ctx) {
  assert(ctx);
  return ctx->iface;
}

Nodes *UastFilter(const Uast *ctx, NodeHandle node, const char *query) {
  assert(ctx);
  assert(node);
  assert(query);

  Nodes *nodes;
  try {
    nodes = new Nodes();
  } catch(const std::bad_alloc&) {
    Error(nullptr, "Unable to get memory for nodes\n");
    return nullptr;
  }

  try {
    QueryResult queryResult(ctx, node, query, XPATH_NODESET);

    auto nodeset = queryResult.xpathObj->nodesetval;
    if (!nodeset) {
      if (NodesSetSize(nodes, 0) != 0) {
        Error(nullptr, "Unable to set nodes size\n");
        throw std::runtime_error("");
      }
      return nodes;
    }

    auto results = nodeset->nodeTab;
    auto size = nodeset->nodeNr;
    size_t realSize = 0;

    for (int i = 0; i < size; i++) {
      if (results[i] != nullptr && results[i]->_private != nullptr) {
        ++realSize;
      }
    }

    if (NodesSetSize(nodes, realSize) != 0) {
      Error(nullptr, "Unable to set nodes size\n");
      throw std::runtime_error("");
    }

    // Populate array of results
    size_t nodeIdx = 0;
    for (int i = 0; i < size; i++) {
      if (results[i] != nullptr && results[i]->_private != nullptr) {
        nodes->results[nodeIdx++] = (NodeHandle)(results[i]->_private);
      }
    }

    return nodes;
  } catch (std::runtime_error&) {
    NodesFree(nodes);
  }

  return nullptr;
}

bool UastFilterBool(const Uast *ctx, NodeHandle node, const char *query,
                    bool *ok) {
  assert(ctx);
  assert(node);
  assert(query);

  try {
    QueryResult queryResult(ctx, node, query, XPATH_BOOLEAN);
    *ok = true;
    return queryResult.xpathObj->boolval;
  } catch (std::runtime_error&) {}

  *ok = false;
  return false;
}

double UastFilterNumber(const Uast *ctx, NodeHandle node, const char *query,
                        bool *ok) {
  assert(ctx);
  assert(node);
  assert(query);

  try {
    QueryResult queryResult(ctx, node, query, XPATH_NUMBER);
    *ok = true;
    return queryResult.xpathObj->floatval;
  } catch (std::runtime_error&) {}

  *ok = false;
  return 0;
}

const char *UastFilterString(const Uast *ctx, NodeHandle node, const char *query) {
  assert(ctx);
  assert(node);
  assert(query);

  try {
    QueryResult queryResult(ctx, node, query, XPATH_STRING);
    char *cstr = reinterpret_cast<char *>(queryResult.xpathObj->stringval);
    if (!cstr) {
      Error(nullptr, "string query returned null string\n");
      return nullptr;
    }
    return strdup(cstr);
  } catch (std::runtime_error&) {}

  return nullptr;
}

char *LastError(void) {
  return strdup(error_message);
}

//////////////////////////////
///////// PRIVATE API ////////
//////////////////////////////

Nodes *NodesNew() { return new Nodes(); }

int NodesSetSize(Nodes *nodes, int len) {
  assert(nodes);

  if (len > nodes->cap) {
    nodes->results.resize(len);
    nodes->cap = len;
  }
  nodes->len = len;
  return 0;
}

int NodesCap(const Nodes *nodes) {
  assert(nodes);

  return nodes->cap;
}

static xmlNodePtr CreateXmlNode(const Uast *ctx, NodeHandle node,
                                xmlNodePtr parent) {
  assert(ctx);
  assert(node);

  char buf[BUF_SIZE];

  const char *internal_type = ctx->iface.InternalType(ctx, node);
  xmlNodePtr xmlNode = static_cast<xmlNodePtr>(xmlNewNode(nullptr, BAD_CAST(internal_type)));
  int children_size = 0;
  int roles_size = 0;
  const char *token = nullptr;

  try {
    if (!xmlNode) {
      throw CreateXMLNodeException();
    }

    xmlNode->_private = (void*)node;
    if (parent) {
      if (!xmlAddChild(parent, xmlNode)) {
        throw CreateXMLNodeException();
      }
    }

    // Token
    token = ctx->iface.Token(ctx, node);
    if (token) {
      if (!xmlNewProp(xmlNode, BAD_CAST("token"), BAD_CAST(token))) {
        throw CreateXMLNodeException();
      }
    }

    // Roles
    roles_size = ctx->iface.RolesSize(ctx, node);
    for (int i = 0; i < roles_size; i++) {
      uint16_t role = ctx->iface.RoleAt(ctx, node, i);
      const char *role_name = RoleNameForId(role);
      if (role_name != nullptr) {
        if (!xmlNewProp(xmlNode, BAD_CAST(role_name), nullptr)) {
          throw CreateXMLNodeException();
        }
      }
    }

    // Properties
    for (size_t i = 0; i < ctx->iface.PropertiesSize(ctx, node); i++) {
      const char *key = ctx->iface.PropertyKeyAt(ctx, node, i);
      const char *value = ctx->iface.PropertyValueAt(ctx, node, i);
      if (!xmlNewProp(xmlNode, BAD_CAST(key), BAD_CAST(value))) {
        throw CreateXMLNodeException();
      }
    }

    // Position
    if (ctx->iface.HasStartOffset(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.StartOffset(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        throw CreateXMLNodeException("Unable to set start offset\n");
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "startOffset", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }
    if (ctx->iface.HasStartLine(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.StartLine(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        throw CreateXMLNodeException("Unable to start line\n");
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "startLine", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }
    if (ctx->iface.HasStartCol(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.StartCol(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        throw CreateXMLNodeException("Unable to start column\n");
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "startCol", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }
    if (ctx->iface.HasEndOffset(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.EndOffset(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        throw CreateXMLNodeException("Unable to set end offset\n");
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "endOffset", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }
    if (ctx->iface.HasEndLine(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.EndLine(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        Error(nullptr, "Unable to set end line\n");
        throw CreateXMLNodeException();
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "endLine", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }
    if (ctx->iface.HasEndCol(ctx, node)) {
      int ret = snprintf(buf, BUF_SIZE, "%" PRIu32, ctx->iface.EndCol(ctx, node));
      if (ret < 0 || ret >= BUF_SIZE) {
        throw CreateXMLNodeException("Unable to set end column\n");
      }
      if (!xmlNewProp(xmlNode, BAD_CAST "endCol", BAD_CAST buf)) {
        throw CreateXMLNodeException();
      }
    }

    // Recursivelly visit all children
    children_size = ctx->iface.ChildrenSize(ctx, node);
    for (int i = 0; i < children_size; i++) {
      NodeHandle child = ctx->iface.ChildAt(ctx, node, i);
      if (!CreateXmlNode(ctx, child, xmlNode)) {
        throw CreateXMLNodeException();
      }
    }
    return xmlNode;
  } catch (CreateXMLNodeException&) {
    xmlFreeNode(xmlNode);
  }

  return nullptr;
}

static xmlDocPtr CreateDocument(const Uast *ctx, NodeHandle node) {
  assert(ctx);
  assert(node);

  auto doc = static_cast<xmlDocPtr>(xmlNewDoc(BAD_CAST("1.0")));
  if (!doc) {
    return nullptr;
  }
  xmlNodePtr xmlNode = CreateXmlNode(ctx, node, nullptr);
  if (!xmlNode) {
    xmlFreeDoc(doc);
    return nullptr;
  }
  xmlDocSetRootElement(doc, xmlNode);
  return doc;
}

void Error(void *ctx, const char *msg, ...) {
  va_list arg_ptr;

  va_start(arg_ptr, msg);
  vsnprintf(error_message, BUF_SIZE, msg, arg_ptr);
  va_end(arg_ptr);
}

static NodeHandle transformChildAt(UastIterator *iter, NodeHandle parent, size_t pos) {
  assert(iter);
  assert(parent);

  auto child = iter->ctx->iface.ChildAt(iter->ctx, parent, pos);
  return iter->nodeTransform ? iter->nodeTransform(child): child;
}

static bool Visited(UastIterator *iter, NodeHandle node) {
  assert(iter);
  assert(node);

  const bool visited = iter->visited.find(node) != iter->visited.end();

  if(!visited) {
    int children_size = iter->ctx->iface.ChildrenSize(iter->ctx, node);
    for (int i = children_size - 1; i >= 0; i--) {
      iter->pending.push_front(transformChildAt(iter, node, i));
    }
    iter->visited.insert(node);
  }

  return visited;
}

static NodeHandle PreOrderNext(UastIterator *iter) {
  assert(iter);

  NodeHandle retNode = iter->pending.front();
  iter->pending.pop_front();

  if (retNode == 0) {
    return 0;
  }

  int children_size = iter->ctx->iface.ChildrenSize(iter->ctx, retNode);
  for (int i = children_size - 1; i >= 0; i--) {
    iter->pending.push_front(transformChildAt(iter, retNode, i));
  }

  return retNode;
}

static NodeHandle LevelOrderNext(UastIterator *iter) {
  assert(iter);

  NodeHandle retNode = iter->pending.front();

  if (retNode == 0) {
    return 0;
  }

  int children_size = iter->ctx->iface.ChildrenSize(iter->ctx, retNode);
  for (int i = 0; i < children_size; i++) {
  iter->pending.push_back(transformChildAt(iter, retNode, i));
}

  iter->pending.pop_front();
  return retNode;
}

static NodeHandle PostOrderNext(UastIterator *iter) {
  assert(iter);

  NodeHandle curNode = iter->pending.front();
  if (curNode == 0) {
    return 0;
  }

  while(!Visited(iter, curNode)) {
    curNode = iter->pending.front();
  }

  curNode = iter->pending.front();
  iter->pending.pop_front();
  return curNode;
}

static void sortPendingByPosition(UastIterator *iter) {
    auto root = iter->pending.front();
    iter->pending.pop_front();

    UastIterator *subiter = UastIteratorNew(iter->ctx, root, PRE_ORDER);
    NodeHandle curNode = 0;
    while ((curNode = UastIteratorNext(subiter)) != 0) {
      iter->pending.push_back(curNode);
    }
    UastIteratorFree(subiter);

    std::sort(iter->pending.begin(), iter->pending.end(), [&iter](NodeHandle i, NodeHandle j) {
      auto ic = iter->ctx->iface;
      if (ic.HasStartOffset(iter->ctx, i) && ic.HasStartOffset(iter->ctx, j)) {
        return ic.StartOffset(iter->ctx, i) < ic.StartOffset(iter->ctx, j);
      }

      // Continue: some didn't have offset, check by line/col
      auto firstLine  = ic.HasStartLine(iter->ctx, i) ? ic.StartLine(iter->ctx, i) : 0;
      auto firstCol   = ic.HasStartCol(iter->ctx, i)  ? ic.StartCol(iter->ctx, i)  : 0;
      auto secondLine = ic.HasStartLine(iter->ctx, j) ? ic.StartLine(iter->ctx, j) : 0;
      auto secondCol  = ic.HasStartCol(iter->ctx, j)  ? ic.StartCol(iter->ctx, j)  : 0;

      if (firstLine == secondLine) {
        return firstCol < secondCol;
      }

      return firstLine < secondLine;
    });
}

static NodeHandle PositionOrderNext(UastIterator *iter) {
  assert(iter);

  if (!iter->preloaded) {
    // First iteration on preorder, storing the nodes in the deque, then sort by pos
    sortPendingByPosition(iter);
    iter->preloaded = true;
  }

  NodeHandle retNode = iter->pending.front();
  if (retNode == 0) {
    return 0;
  }

  iter->pending.pop_front();
  return retNode;
}
