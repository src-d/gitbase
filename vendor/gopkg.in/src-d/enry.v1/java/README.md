# enry-java

## Usage

`enry-java` package is available thorugh [maven central](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22enry-java%22),
so it be used easily added as a dependency in various package management systems.
Examples of how to handle it for most commons systems are included below,
for other systems just look at maven central's dependency information.

### Apache Maven

```xml
<dependency>
    <groupId>tech.sourced</groupId>
    <artifactId>enry-java</artifactId>
    <version>${enry_version}</version>
</dependency>
```

### Scala SBT

```scala
libraryDependencies += "tech.sourced" % "enry-java" % enryVersion
```

## Build

### Requirements

* `sbt`
* `Java` (tested with Java 1.8)
* `wget`
* `Go` (only for building the shared objects for your operating system)

### Generate jar with Java bindings and shared libraries

You need to do this before exporting the jar and/or testing.

```
make
```

This will download JNAerator jar to generate the code from the `libenry.h` header file, it will be placed under `lib`.
The shared libraries for your operating system will be built if needed and copied inside the `shared` directory.

For IntelliJ and other IDEs remember to mark `shared` folder as sources and add `lib/enry.jar` as library. If you use `sbt` from the command line directly that's already taken care of.

### Run tests

```
make test
```

### Export jar

```
make package
```

Will build fatJar under `./target/enry-java-assembly-X.X.X.jar`.
One can use `./sbt publish-local` to install enry-java dependency on local machine.
