package siva // import "gopkg.in/src-d/go-siva.v1"

// siva files are n number of blocks with the following format:
//
// - n number of raw content, without any restriction and without any divider
// - 3-byte index header, {'I', 'B,' A'}, the begining of the index section
// - n number of index entries, the index entry looks like:
//      4-byte length of the filename
//      n-byte filename
//      4-byte permission and mode bits
//      8-byte mod time in nanoseconds
//      8-byte offset to the start of the file content in the current block
//      8-byte size of the file
//      4-byte CRC32 of file content
//      4-byte flags
// - x-byte index footer
//      4-byte entries count
//      8-byte index size
//      8-byte block size
//      4-byte CRC32 of index header + index entries
//
//     +------------------+
//     | raw file content | ----------+
//     +------------------+           |
//     |       ...        |           |
//     +------------------+           |
//     | raw file content |           |
//     +------------------+           |
//     |   index header   | --+       |
//     +------------------+   |       | block
//     |   index entry    |   |       |
//     +------------------+   |       |
//     |       ...        |   | index |
//     +------------------+   |       |
//     |   index entry    |   |       |
//     +------------------+   |       |
//     |   index footer   | --+-------+
//     +------------------+
