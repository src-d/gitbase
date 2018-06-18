# Importing and Exporting Data

## Importing Data

### Overview

If you have large amounts of data, it is more efficient to import it into Pilosa instead of using multiple SetBit queries. This library supports importing anything which implements the `Record` interface, such as `Bit`s or `FieldValue`s into an existing frame.

Once you create the frame in which to import data, you need to create an instance of a struct which implements the `RecordIterator` interface. This library ships with [CSVIterator](#csv-iterator).

Finally you should call `client.ImportFrame` or `client.ImportValueFrame` with the necessary parameters to start the import process.

### A Simple Example

Let's use the `CSVBitIterator` which reads `Bit` records from a `CSV` reader:
```go
// Ensure frame exists.
// ...
// Here is some CSV.
text := `10,7
         10,5
         2,3
         7,1`
// Create the iterator
iterator := pilosa.NewCSVBitIterator(strings.NewReader(text))
// Start the import
err := client.ImportFrame(frame, iterator)
if err != nil {
	log.Fatal(err)
}
```

It is possible to set import options, e.g., the number of goroutines and also track the status of the import. We are going to see how to accomplish those in the [Advanced Usage](#advanced-usage) section.

### <a name="csv-iterator"></a>CSVIterator

The `CSVIterator` reads lines from an `io.Reader` and converts them to `Record`s using a `BitCSVUnmarshaller`.

The expected CSV format is:
```
FIELD1,FIELD2,...
```

Note that each line corresponds to a single bit and ends with a new line (`\n` or `\r\n`), except the last line (which is optional).

`BitCSVUnmarshaller` is any function which takes some string and returns a `Record` or `error`. It is defined as follows:
```go
type CSVRecordUnmarshaller func(text string) (Record, error)
```

We have three predefined `CSVRecordUnmarshaller`s in this library: `BitCSVUnmarshaller`, `BitCSVUnmarshallerWithTimestamp` and `FieldValueCSVUnmarshaller` which are explained in the subsections below.

#### BitCSVUnmarshaller and CSVBitIterator

`CSVBitIterator` is a `CSVIterator` which uses the `BitCSVUnmarshaller` as the unmarshaller. `BitCSVUnmarshaller` unmarshals CSV rows with the default timestamp format.

A row with the following format is expected:
```
ROW_ID,COLUMN_ID
```

Optionally, a timestamp can be added. Note that Pilosa is not time zone aware:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

Example:
```go
iterator := NewCSVBitIterator(strings.NewReader(text))
```

#### BitCSVUnmarshallerWithTimestamp and CSVBitIteratorWithTimestamp

`CSVBitIterator` is a `CSVIterator` which uses the `BitCSVUnmarshallerWithTimestamp` as the unmarshaller. `BitCSVUnmarshallerWithTimestamp` unmarshals CSV using the given timestamp format.

Example:
```go
format := "2006-01-02T03:04"
iterator := pilosa.NewCSVBitIteratorWithTimestampFormat(reader, format)
```

#### FieldValueCSVUnmarshaller and CSVValueIterator

`CSVFieldValueIterator` is a `BitCSVUnmarshaller` which can read CSV rows suitable to be imported into [BSI fields](https://www.pilosa.com/docs/latest/data-model/#bsi-range-encoding).

A row with the following format is expected:
```
COLUMN_ID,INTEGER_VALUE
```

Example:
```go
iterator := NewCSVValueIterator(strings.NewReader(text))
```

### RecordIterator

In case your data is not coming from a CSV data source (*highly likely!*) you need to write your own struct which implements the `RecordIterator` interface. The `RecordIterator` is defined as follows:
```go
type RecordIterator interface {
	NextRecord() (Record, error)
}
```
So, a `RecordIterator` returns either the next `Record` when its `NextRecord` function is called, or an `error`. If there is no other `Record` to return, `NextRecord` returns `io.EOF.

A record is a struct instance which implements the `Record` interface. Currently that means `Bit` and `FieldValue` structs.

Let's define a simple `RecordIterator` which returns a predefined number of random `Bit`s:
```go
type RandomBitGenerator struct {
	maxRowID uint64
	maxColumnID uint64
	maxBits int
}

func (gen *RandomBitGenerator) NextRecord() (Record, error) {
	if gen.maxBits <= 0 {
		return nil, io.EOF
	}
	gen.maxBits -= 1
	return Bit{
		RowID: rand.Uint64n(gen.maxRowID),
		ColumnID: rand.Uint64n(gen.maxColumnID),
	}, nil
}
```

If you intend to import values for a range field, return `FieldValue`s instead of `Bit`s:
```
func (gen *RandomBitGenerator) NextRecord() (Record, error) {
	// ...
	return FieldValue{
		ColumnID: rand.Uint64n(gen.maxRowID),
		Value: 42,
	}, nil
}
```

### <a name="advanced-usage"></a>Advanced Usage

#### Import Options

You can change the import strategy, thread count and other options by passing them to `client.ImportFrame` or `client.ImportFrameWithStatus` functions. Here are the import options:
* `OptImportStrategy`: Changes the import strategy of the import goroutines to one of the following:
	* `DefaultImport`: Default strategy, currently `TimeoutImport`.
	* `BatchImport`: Read `BatchSize` records, bucket them by slices and import them. By default 100000.
	* `TimeoutImport`: Read and bucket records by slices and after `Timeout` import the largest bucket. By default `100` milliseconds.
* `OptImportThreadCount`: Number of import goroutines. By default only a single importer is used.
* `OptImportBatchSize`: Sets the `BatchSize`.
* `OptImportTimeout`: Sets the `Timeout`.
* `OptImportStatusChannel`: Sets the status channel to track the import progress.

Here's how you would set import options:
```go
err := client.ImportFrame(frame, iterator,
	OptImportThreadCount(4),
	OptImportStrategy(TimeoutImport),
	OptImportTimeout(200 * time.Millisecond))
```

### Tracking Import Status

You can pass a channel of type `ImportStatusUpdate` to `client.ImportFrame` using `OptImportStatusChannel` function to get notified when an importer imports a slice of bits. The status channel is closed by the client when the import 
ends.

Note that if you use this feature, you have to consume from the status channel, otherwise import goroutines may get blocked.

`ImportStatusUpdate` is defined as follows:
```go
type ImportStatusUpdate struct {
	ThreadID      int  // goroutine index
	Slice         uint64 // slice that was imported
	ImportedCount int // imported number of bits
	Time          time.Duration // the time it took to import
}
```

Run the import process in a goroutine in order to be able to read from the status channel and act on it. Here's an example:
```go
statusChan := make(chan pilosa.ImportStatusUpdate, 1000)
go func() {
	err := client.ImportFrame(f1, bitIterator, pilosa.OptImportStatusChannel(statusChan), pilosa.OptImportThreadCount(2))
	if err != nil {
		log.Fatal(err)
	}
}()

var status pilosa.ImportStatusUpdate
totalImported := 0
tic := time.Now()
ok := true
for ok {
	select {
	case status, ok = <-statusChan:
		if !ok {
			break
		}
		// act on the status update
	default:
		// do something while waiting for the next status update to arrive.
		time.Sleep(1000 * time.Millisecond)
	}
}
```

## Exporting Data

You can export a view of a frame from Pilosa using `client.ExportFrame` function which returns a `BitIterator`. Use the `NextBit` function of this iterator to receive all bits for the specified frame. When there are no more bits, `io.EOF` is returned.

The `PilosaClient` struct has the `Views` function which returns all of the views for a particular frame. You can use this function to retrieve view names:
```go
views, err := client.Views(frame)
```

Here's sample code which retrieves bits of the `standard` view:

```go
iterator, err := client.ExportFrame(frame, "standard")
if err != nil {
    log.Fatal(err)
}

bits := []pilosa.Bit{}
for {
    bit, err := iterator.NextBit()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    bits = append(bits, bit)
}
```
