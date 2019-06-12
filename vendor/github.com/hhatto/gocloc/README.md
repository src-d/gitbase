# gocloc [![travis-ci](https://travis-ci.org/hhatto/gocloc.svg?branch=master)](https://travis-ci.org/hhatto/gocloc)

A little fast [cloc(Count Lines Of Code)](https://github.com/AlDanial/cloc), written in Go.
Inspired by [tokei](https://github.com/Aaronepower/tokei).

## Installation

```
$ go get -u github.com/hhatto/gocloc/cmd/gocloc
```

## Usage

### Basic Usage
```
$ gocloc .
```

```
$ gocloc .
-------------------------------------------------------------------------------
Language                     files          blank        comment           code
-------------------------------------------------------------------------------
Markdown                         3              8              0             18
Go                               1             29              1            323
-------------------------------------------------------------------------------
TOTAL                            4             37              1            341
-------------------------------------------------------------------------------
```

### Integration Jenkins CI
use [SLOCCount Plugin](https://wiki.jenkins-ci.org/display/JENKINS/SLOCCount+Plugin).

```
$ cloc --by-file --output-type=sloccount . > sloccount.scc
```

```
$ cat sloccount.scc
398 Go      ./main.go
190 Go      ./language.go
132 Markdown        ./README.md
24  Go      ./xml.go
18  Go      ./file.go
15  Go      ./option.go
```

## Support Languages
use `--show-lang` option

```
$ gocloc --show-lang
```

## Performance
* CPU 3.1GHz Intel Core i7 / 16GB 1600MHz DDR3 / MacOSX 10.11.3
* cloc 1.66
* tokei 1.5.1
* gocloc [6a9d4f5](https://github.com/hhatto/gocloc/commit/6a9d4f5b3d4e5df28fe78a04e8741595e22ada50)
* target repository is [golang/go commit:633ab74](https://github.com/golang/go/tree/633ab7426a906b72dcf6f1d54e87f4ae926dc4e1)

### cloc

```
$ time cloc .
    5171 text files.
    5052 unique files.
     420 files ignored.

https://github.com/AlDanial/cloc v 1.66  T=23.31 s (204.0 files/s, 48203.3 lines/s)
-----------------------------------------------------------------------------------
Language                         files          blank        comment           code
-----------------------------------------------------------------------------------
Go                                4197         101140         125939         780280
Assembly                           331           6128          14654          40531
HTML                                41           4927            198          33316
C                                   90           1076            940           7390
Perl                                12            185            177           1135
Bourne Again Shell                  25            209            266            933
XML                                  4             85              9            623
Bourne Shell                         8             71            302            467
Python                               1            121             88            295
DOS Batch                            5             55              1            238
JavaScript                           4             48            122            231
C/C++ Header                        15             50            147            211
CSS                                  3             51              9            176
yacc                                 1             27             20            155
Protocol Buffers                     1              1              0            144
Windows Resource File                4             25              0            116
JSON                                 2              0              0             36
make                                 7             12             10             35
Fortran 90                           2              1              3              8
C++                                  1              3              5              7
awk                                  1              1              6              7
-----------------------------------------------------------------------------------
SUM:                              4755         114216         142896         866334
-----------------------------------------------------------------------------------
cloc .  13.57s user 7.89s system 105% cpu 20.413 total
```

### tokei

```
$ time tokei --sort code .
-------------------------------------------------------------------------------
 Language            Files        Total       Blanks     Comments         Code
-------------------------------------------------------------------------------
 Go                   4272      1027537       103241       150411       773970
 Plain Text             28            0            0            0       137715
 Assembly              334        61318         6130            0        55188
 HTML                   41        38441         4927          204        33316
 C                      92         9436         1081          946         7409
 BASH                   27         2134          260          570         1304
 XML                     4          717           85            9          623
 Perl                   10         1255          151         1096          343
 Python                  1          504          121           56          327
 Batch                   5          294           55            0          239
 JavaScript              4          401           48          122          231
 C Header               15          408           50          147          211
 CSS                     3          236           51            9          176
 Protocol Buffers        1          145            1            0          144
 Markdown                3            0            0            0          115
 JSON                    2            0            0            0           36
 Makefile                7           57           13           10           34
 FORTRAN Modern          2           12            1            3            8
 C++                     1           15            3            5            7
-------------------------------------------------------------------------------
 Total                4852      1142910       116218       153588      1011396
-------------------------------------------------------------------------------
tokei --sort code .  1.27s user 0.06s system 99% cpu 1.328 total
```

### gocloc

```
$ time gocloc --exclude-ext=txt .
-------------------------------------------------------------------------------
Language                     files          blank        comment           code
-------------------------------------------------------------------------------
Go                            4197         101140         133703         773058
Assembly                       331           6128          14694          40510
HTML                            41           4927            198          33316
C                               90           1076            991           7389
BASH                            28            257            539           1330
Perl                            12            185            177           1135
XML                              4             85              9            623
Python                           1            121             88            295
Batch                            5             55              1            238
JavaScript                       4             48            122            231
C Header                        15             50            147            211
CSS                              3             51              9            176
Yacc                             1             27             20            155
Protocol Buffers                 1              1              0            144
Markdown                         3             29              0             86
Plan9 Shell                      4             25             42             74
Bourne Shell                     5             23             29             70
JSON                             2              0              0             36
make                             7             13             10             34
FORTRAN Modern                   2              1              3              8
Awk                              1              1              6              7
C++                              1              3              5              7
-------------------------------------------------------------------------------
TOTAL                         4758         114246         150793         859133
-------------------------------------------------------------------------------
gocloc --exclude-ext=txt .  0.76s user 0.15s system 116% cpu 0.775 total
```

## License
MIT
