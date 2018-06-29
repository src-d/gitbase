#ifndef EXPORT
#if !defined(LIBUAST_STATIC) && (defined(WIN32) || defined(_WIN32) || defined(__WIN32) && !defined(__CYGWIN__))
#ifdef LIBUAST_BUILD
#define EXPORT __declspec(dllexport)
#else
#define EXPORT __declspec(dllimport)
#endif
#else
#define EXPORT
#endif
#endif
