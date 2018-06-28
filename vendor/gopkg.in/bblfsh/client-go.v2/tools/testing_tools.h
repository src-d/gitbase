#ifndef LIBUAST_TESTING_TOOLS_H_
#define LIBUAST_TESTING_TOOLS_H_

#ifdef TESTING

#include <stdbool.h>
#include <stdlib.h>

#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

extern bool fail_xmlNewNode;
extern bool fail_xmlNewDoc;
extern bool fail_xmlNewProc;
extern bool fail_xmlAddChild;
extern bool fail_xmlXPathNewContext;

void *MockXmlNewNode(xmlNsPtr ns, const xmlChar *name);
#define xmlNewNode MockXmlNewNode

void *MockXmlNewDoc(const xmlChar *xmlVersion);
#define xmlNewDoc MockXmlNewDoc

void *MockXmlNewProp(xmlNodePtr node, const xmlChar *name,
                     const xmlChar *value);
#define xmlNewProp MockXmlNewProp

void *MockXmlAddChild(xmlNodePtr parent, xmlNodePtr cur);
#define xmlAddChild MockXmlAddChild

void *MockXmlXPathNewContext(xmlDocPtr doc);
#define xmlXPathNewContext MockXmlXPathNewContext

#endif  // TESTING
#endif  // LIBUAST_TESTING_TOOLS_H_
