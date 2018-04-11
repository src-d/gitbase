# Installing client-go on Windows

There are two supported options how to build client-go on Windows:
static linking using MinGW or dynamic linking with Visual Studio.
The former does not require any external DLL files at runtime and is "Go style".

## Prerequisites

Same as for [libuast](https://github.com/src-d/libuast/blob/master/WINDOWS.md)
and additionally:

* MinGW is always required because CGo expects a GNU compiler under the hood.
* MSVC++ option requires [pexports](https://sourceforge.net/projects/mingw/files/MinGW/Extension/pexports/pexports-0.47/)

The following options correspond to how `libuast` was built: CGo always
uses MinGW internally.

## MinGW static

```
SET CGO_CFLAGS=-I%PREFIX%\include -DLIBUAST_STATIC
SET CGO_LDFLAGS=-L%PREFIX%\lib -luast -lxml2 -static -lstdc++ -static-libgcc
go get -v -tags custom_libuast gopkg.in/bblfsh/client-go.v2/...
```

`-static-libstdc++` instead of `-static -lstdc++` may be used if works.

## MSVC++ dynamic

We use `dlltool` to build the interface static libraries to call the foreign DLLs.

```
pexports %PREFIX%/bin/libxml2.dll > %PREFIX%/bin/libxml2.def
dlltool -k --no-leading-underscore -d %PREFIX%/bin/libxml2.def -l %PREFIX%/lib/libxml2.a
pexports %PREFIX%/bin/uast.dll > %PREFIX%/bin/uast.def
dlltool -k --no-leading-underscore -d %PREFIX%/bin/libxml2.def -l %PREFIX%/lib/libuast.a

SET CGO_CFLAGS=-I%PREFIX%\include
SET CGO_LDFLAGS=-L%PREFIX%\lib -luast -lxml2
go get -v -tags custom_libuast gopkg.in/bblfsh/client-go.v2/...
```

You have to carry `%PREFIX%\bin\libxml2.dll` and `%PREFIX%\bin\uast.dll`
to the path where you call a Go binary which depends on client-go.
You also need to install [Microsoft Visual C++ Redistributable](https://www.visualstudio.com/downloads/#title-39324)
on clients.