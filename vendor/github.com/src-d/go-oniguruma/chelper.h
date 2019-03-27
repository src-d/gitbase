#include <oniguruma.h>

extern void init();

extern int CompileAndMatch2(char *pattern, char *str);

extern int NewOnigRegex2(char *pattern, int pattern_length, OnigRegex* regex, char **error_buffer);

extern int SearchOnigRegex2(void *str, int str_length, int offset, OnigRegex regex);

extern int MatchOnigRegex2(void *str, int str_length, int offset, OnigRegex regex);

extern int NewOnigRegex( char *pattern, int pattern_length, int option,
                                  OnigRegex *regex, OnigRegion **region, OnigEncoding *encoding, OnigErrorInfo **error_info, char **error_buffer);

extern int SearchOnigRegex( void *str, int str_length, int offset, int option,
                                  OnigRegex regex, OnigRegion *region, OnigErrorInfo *error_info, char *error_buffer, int *captures, int *numCaptures);

extern int MatchOnigRegex( void *str, int str_length, int offset, int option,
                  OnigRegex regex, OnigRegion *region);

extern int LookupOnigCaptureByName(char *name, int name_length, OnigRegex regex, OnigRegion *region);

extern int GetCaptureNames(OnigRegex regex, void *buffer, int bufferSize, int* groupNumbers);
