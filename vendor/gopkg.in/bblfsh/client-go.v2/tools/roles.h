#ifndef LIBUAST_ROLES_H_
#define LIBUAST_ROLES_H_

#include <stdint.h>
#include "export.h"

#ifdef __cplusplus
extern "C" {
#endif

EXPORT const char *RoleNameForId(uint16_t id);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // LIBUAST_ROLES_H_
