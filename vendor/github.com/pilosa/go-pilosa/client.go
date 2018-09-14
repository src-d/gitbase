// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const PqlVersion = "1.0"
const maxHosts = 10

// Client is the HTTP client for Pilosa server.
type Client struct {
	cluster *Cluster
	client  *http.Client
	// User-Agent header cache. Not used until cluster-resize support branch is merged
	// and user agent is saved here in NewClient
	userAgent              string
	importThreadCount      int
	shardWidth             uint64
	fragmentNodeCache      map[string][]fragmentNode
	fragmentNodeCacheMutex *sync.RWMutex
	importManager          *recordImportManager
	logger                 *log.Logger
}

// DefaultClient creates a client with the default address and options.
func DefaultClient() *Client {
	return newClientWithCluster(NewClusterWithHost(DefaultURI()), nil)
}

func newClientFromAddresses(addresses []string, options *ClientOptions) (*Client, error) {
	uris := make([]*URI, len(addresses))
	for i, address := range addresses {
		uri, err := NewURIFromAddress(address)
		if err != nil {
			return nil, err
		}
		uris[i] = uri
	}
	cluster := NewClusterWithHost(uris...)
	client := newClientWithCluster(cluster, options)
	return client, nil
}

func newClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	options = options.withDefaults()
	c := &Client{
		cluster:                cluster,
		client:                 newHTTPClient(options.withDefaults()),
		fragmentNodeCache:      map[string][]fragmentNode{},
		fragmentNodeCacheMutex: &sync.RWMutex{},
		logger:                 log.New(os.Stderr, "go-pilosa ", log.Flags()),
	}
	c.importManager = newRecordImportManager(c)
	return c
}

// NewClient creates a client with the given address, URI, or cluster and options.
func NewClient(addrUriOrCluster interface{}, options ...ClientOption) (*Client, error) {
	var cluster *Cluster
	clientOptions := &ClientOptions{}
	err := clientOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}

	switch u := addrUriOrCluster.(type) {
	case string:
		uri, err := NewURIFromAddress(u)
		if err != nil {
			return nil, err
		}
		cluster = NewClusterWithHost(uri)
	case []string:
		return newClientFromAddresses(u, clientOptions)
	case *URI:
		cluster = NewClusterWithHost(u)
	case []*URI:
		cluster = NewClusterWithHost(u...)
	case *Cluster:
		cluster = u
	case nil:
		cluster = NewClusterWithHost()
	default:
		return nil, ErrAddrURIClusterExpected
	}

	return newClientWithCluster(cluster, clientOptions), nil
}

// Query runs the given query against the server with the given options.
// Pass nil for default options.
func (c *Client) Query(query PQLQuery, options ...interface{}) (*QueryResponse, error) {
	if err := query.Error(); err != nil {
		return nil, err
	}
	queryOptions := &QueryOptions{}
	err := queryOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	data, err := makeRequestData(query.serialize(), queryOptions)
	if err != nil {
		return nil, errors.Wrap(err, "making request data")
	}
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, buf, err := c.httpRequest("POST", path, data, defaultProtobufHeaders())
	if err != nil {
		return nil, err
	}
	iqr := &pbuf.QueryResponse{}
	err = proto.Unmarshal(buf, iqr)
	if err != nil {
		return nil, err
	}
	queryResponse, err := newQueryResponseFromInternal(iqr)
	if err != nil {
		return nil, err
	}
	return queryResponse, nil
}

// CreateIndex creates an index on the server using the given Index struct.
func (c *Client) CreateIndex(index *Index) error {
	data := []byte(index.options.String())
	path := fmt.Sprintf("/index/%s", index.name)
	response, _, err := c.httpRequest("POST", path, data, nil)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrIndexExists
		}
		return err
	}
	return nil

}

// CreateField creates a field on the server using the given Field struct.
func (c *Client) CreateField(field *Field) error {
	data := []byte(field.options.String())
	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	response, _, err := c.httpRequest("POST", path, data, nil)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrFieldExists
		}
		return err
	}
	return nil
}

// EnsureIndex creates an index on the server if it does not exist.
func (c *Client) EnsureIndex(index *Index) error {
	err := c.CreateIndex(index)
	if err == ErrIndexExists {
		return nil
	}
	return err
}

// EnsureField creates a field on the server if it doesn't exists.
func (c *Client) EnsureField(field *Field) error {
	err := c.CreateField(field)
	if err == ErrFieldExists {
		return nil
	}
	return err
}

// DeleteIndex deletes an index on the server.
func (c *Client) DeleteIndex(index *Index) error {
	path := fmt.Sprintf("/index/%s", index.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil)
	return err

}

// DeleteField deletes a field on the server.
func (c *Client) DeleteField(field *Field) error {
	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil)
	return err
}

// SyncSchema updates a schema with the indexes and fields on the server and
// creates the indexes and fields in the schema on the server side.
// This function does not delete indexes and the fields on the server side nor in the schema.
func (c *Client) SyncSchema(schema *Schema) error {
	serverSchema, err := c.Schema()
	if err != nil {
		return err
	}

	return c.syncSchema(schema, serverSchema)
}

func (c *Client) syncSchema(schema *Schema, serverSchema *Schema) error {
	var err error

	// find out local - remote schema
	diffSchema := schema.diff(serverSchema)
	// create the indexes and fields which doesn't exist on the server side
	for indexName, index := range diffSchema.indexes {
		if _, ok := serverSchema.indexes[indexName]; !ok {
			err = c.EnsureIndex(index)
			if err != nil {
				return err
			}
		}
		for _, field := range index.fields {
			err = c.EnsureField(field)
			if err != nil {
				return err
			}
		}
	}

	// find out remote - local schema
	diffSchema = serverSchema.diff(schema)
	for indexName, index := range diffSchema.indexes {
		if localIndex, ok := schema.indexes[indexName]; !ok {
			schema.indexes[indexName] = index
		} else {
			for fieldName, field := range index.fields {
				localIndex.fields[fieldName] = field
			}
		}
	}

	return nil
}

// Schema returns the indexes and fields on the server.
func (c *Client) Schema() (*Schema, error) {
	var indexes []StatusIndex
	indexes, err := c.readSchema()
	if err != nil {
		return nil, err
	}
	schema := NewSchema()
	for _, indexInfo := range indexes {
		index := schema.indexWithOptions(indexInfo.Name, indexInfo.Options.asIndexOptions())
		for _, fieldInfo := range indexInfo.Fields {
			index.fieldWithOptions(fieldInfo.Name, fieldInfo.Options.asFieldOptions())
		}
	}
	return schema, nil
}

// ImportField imports records from the given iterator.
func (c *Client) ImportField(field *Field, iterator RecordIterator, options ...ImportOption) error {
	importOptions := &ImportOptions{}
	if field.options != nil && field.options.fieldType == FieldTypeInt {
		importRecordsFunction(c.importValues)(importOptions)
	} else {
		importRecordsFunction(c.importColumns)(importOptions)
	}
	for _, option := range options {
		if err := option(importOptions); err != nil {
			return err
		}
	}
	return c.importManager.Run(field, iterator, importOptions.withDefaults())
}

func (c *Client) importColumns(indexName string, fieldName string, shard uint64, records []Record) error {
	nodes, err := c.fetchFragmentNodes(indexName, shard)
	if err != nil {
		return errors.Wrap(err, "fetching fragment nodes")
	}
	eg := errgroup.Group{}
	for _, node := range nodes {
		uri := &URI{
			scheme: node.Scheme,
			host:   node.Host,
			port:   node.Port,
		}
		eg.Go(func() error {
			return c.importNode(uri, columnsToImportRequest(indexName, fieldName, shard, records))
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing columns to nodes")
}

func (c *Client) importValues(indexName string, fieldName string, shard uint64, vals []Record) error {
	nodes, err := c.fetchFragmentNodes(indexName, shard)
	if err != nil {
		return err
	}
	eg := errgroup.Group{}
	for _, node := range nodes {
		uri := &URI{
			scheme: node.Scheme,
			host:   node.Host,
			port:   node.Port,
		}
		eg.Go(func() error {
			return c.importValueNode(uri, valsToImportRequest(indexName, fieldName, shard, vals))
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing values to nodes")
}

func (c *Client) fetchFragmentNodes(indexName string, shard uint64) ([]fragmentNode, error) {
	key := fmt.Sprintf("%s-%d", indexName, shard)
	c.fragmentNodeCacheMutex.RLock()
	nodes, ok := c.fragmentNodeCache[key]
	c.fragmentNodeCacheMutex.RUnlock()
	if ok {
		return nodes, nil
	}
	path := fmt.Sprintf("/internal/fragment/nodes?shard=%d&index=%s", shard, indexName)
	_, body, err := c.httpRequest("GET", path, []byte{}, nil)
	if err != nil {
		return nil, err
	}
	fragmentNodes := []fragmentNode{}
	var fragmentNodeURIs []fragmentNodeRoot
	err = json.Unmarshal(body, &fragmentNodeURIs)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling fragment node URIs")
	}
	for _, nodeURI := range fragmentNodeURIs {
		fragmentNodes = append(fragmentNodes, nodeURI.URI)
	}
	c.fragmentNodeCacheMutex.Lock()
	c.fragmentNodeCache[key] = fragmentNodes
	c.fragmentNodeCacheMutex.Unlock()
	return fragmentNodes, nil
}

func (c *Client) importNode(uri *URI, request *pbuf.ImportRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "marshaling to protobuf")
	}
	return c.importData(uri, request.GetIndex(), request.GetField(), data)
}

func (c *Client) importValueNode(uri *URI, request *pbuf.ImportValueRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "marshaling to protobuf")
	}
	return c.importData(uri, request.GetIndex(), request.GetField(), data)
}

func (c *Client) importData(uri *URI, indexName string, fieldName string, data []byte) error {
	path := fmt.Sprintf("/index/%s/field/%s/import", indexName, fieldName)
	resp, err := c.doRequest(uri, "POST", path, defaultProtobufHeaders(), bytes.NewReader(data))
	if err = anyError(resp, err); err != nil {
		return errors.Wrap(err, "doing import")
	}
	defer resp.Body.Close()

	return nil
}

// ExportField exports columns for a field.
func (c *Client) ExportField(field *Field) (RecordIterator, error) {
	var shardsMax map[string]uint64
	var err error

	status, err := c.Status()
	if err != nil {
		return nil, err
	}
	shardsMax, err = c.shardsMax()
	if err != nil {
		return nil, err
	}
	status.indexMaxShard = shardsMax
	shardURIs, err := c.statusToNodeShardsForIndex(status, field.index.Name())
	if err != nil {
		return nil, err
	}
	return NewCSVColumnIterator(newExportReader(c, shardURIs, field)), nil
}

// Status returns the serves status.
func (c *Client) Status() (Status, error) {
	_, data, err := c.httpRequest("GET", "/status", nil, nil)
	if err != nil {
		return Status{}, errors.Wrap(err, "requesting /status")
	}
	status := Status{}
	err = json.Unmarshal(data, &status)
	if err != nil {
		return Status{}, errors.Wrap(err, "unmarshaling /status data")
	}
	return status, nil
}

func (c *Client) readSchema() ([]StatusIndex, error) {
	_, data, err := c.httpRequest("GET", "/schema", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /schema")
	}
	schemaInfo := SchemaInfo{}
	err = json.Unmarshal(data, &schemaInfo)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /schema data")
	}
	return schemaInfo.Indexes, nil
}

func (c *Client) shardsMax() (map[string]uint64, error) {
	_, data, err := c.httpRequest("GET", "/internal/shards/max", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /internal/shards/max")
	}
	m := map[string]map[string]uint64{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /internal/shards/max data")
	}
	return m["standard"], nil
}

// HttpRequest sends an HTTP request to the Pilosa server.
// **NOTE**: This function is experimental and may be removed in later revisions.
func (c *Client) HttpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
	return c.httpRequest(method, path, data, headers)
}

// httpRequest makes a request to the cluster - use this when you want the
// client to choose a host, and it doesn't matter if the request goes to a
// specific host
func (c *Client) httpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
	if data == nil {
		data = []byte{}
	}

	// try at most maxHosts non-failed hosts; protect against broken cluster.removeHost
	var response *http.Response
	var err error
	for i := 0; i < maxHosts; i++ {
		reader := bytes.NewReader(data)
		// get a host from the cluster
		host := c.cluster.Host()
		if host == nil {
			return nil, nil, ErrEmptyCluster
		}

		response, err = c.doRequest(host, method, path, c.augmentHeaders(headers), reader)
		if err == nil {
			break
		}
		c.cluster.RemoveHost(host)
	}
	if response == nil {
		return nil, nil, ErrTriedMaxHosts
	}
	defer response.Body.Close()
	warning := response.Header.Get("warning")
	if warning != "" {
		c.logger.Println(warning)
	}
	// TODO: Optimize buffer creation
	buf, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err := NewError(fmt.Sprintf("Server error (%d) %s: %s", response.StatusCode, response.Status, string(buf)))
		return response, buf, err
	}
	return response, buf, nil
}

// anyError checks an http Response and error to see if anything went wrong with
// a request (either locally, or on the server) and returns a single error if
// so.
func anyError(resp *http.Response, err error) error {
	if err != nil {
		return errors.Wrap(err, "unable to perform request")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "bad status '%s' and err reading body", resp.Status)
		}
		msg := string(buf)
		return errors.Errorf("Server error %s body:'%s'", resp.Status, msg)
	}
	return nil
}

// doRequest creates and performs an http request.
func (c *Client) doRequest(host *URI, method, path string, headers map[string]string, reader io.Reader) (*http.Response, error) {
	req, err := makeRequest(host, method, path, headers, reader)
	if err != nil {
		return nil, errors.Wrap(err, "building request")
	}
	return c.client.Do(req)
}

// statusToNodeShardsForIndex finds the hosts which contains shards for the given index
func (c *Client) statusToNodeShardsForIndex(status Status, indexName string) (map[uint64]*URI, error) {
	result := make(map[uint64]*URI)
	if maxShard, ok := status.indexMaxShard[indexName]; ok {
		for shard := 0; shard <= int(maxShard); shard++ {
			fragmentNodes, err := c.fetchFragmentNodes(indexName, uint64(shard))
			if err != nil {
				return nil, err
			}
			if len(fragmentNodes) == 0 {
				return nil, ErrNoFragmentNodes
			}
			node := fragmentNodes[0]
			uri := &URI{
				host:   node.Host,
				port:   node.Port,
				scheme: node.Scheme,
			}

			result[uint64(shard)] = uri
		}
	} else {
		return nil, ErrNoShard
	}
	return result, nil
}

func (c *Client) augmentHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		headers = map[string]string{}
	}

	// TODO: move the following block to NewClient once cluster-resize support branch is merged.
	version := Version
	if strings.HasPrefix(version, "v") {
		version = version[1:]
	}

	headers["User-Agent"] = fmt.Sprintf("go-pilosa/%s", version)
	return headers
}

func defaultProtobufHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
		"PQL-Version":  PqlVersion,
	}
}

func makeRequest(host *URI, method, path string, headers map[string]string, reader io.Reader) (*http.Request, error) {
	request, err := http.NewRequest(method, host.Normalize()+path, reader)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	return request, nil
}

func newHTTPClient(options *ClientOptions) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: options.ConnectTimeout,
		}).Dial,
		TLSClientConfig:     options.TLSConfig,
		MaxIdleConnsPerHost: options.PoolSizePerRoute,
		MaxIdleConns:        options.TotalPoolSize,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   options.SocketTimeout,
	}
}

func makeRequestData(query string, options *QueryOptions) ([]byte, error) {
	request := &pbuf.QueryRequest{
		Query:           query,
		Shards:          options.Shards,
		ColumnAttrs:     options.ColumnAttrs,
		ExcludeRowAttrs: options.ExcludeRowAttrs,
		ExcludeColumns:  options.ExcludeColumns,
	}
	r, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling request to protobuf")
	}
	return r, nil
}

func columnsToImportRequest(indexName string, fieldName string, shard uint64, records []Record) *pbuf.ImportRequest {
	rowIDs := make([]uint64, 0, len(records))
	columnIDs := make([]uint64, 0, len(records))
	timestamps := make([]int64, 0, len(records))
	for _, record := range records {
		column := record.(Column)
		rowIDs = append(rowIDs, column.RowID)
		columnIDs = append(columnIDs, column.ColumnID)
		timestamps = append(timestamps, column.Timestamp)
	}
	return &pbuf.ImportRequest{
		Index:      indexName,
		Field:      fieldName,
		Shard:      shard,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		Timestamps: timestamps,
	}
}

func valsToImportRequest(indexName string, fieldName string, shard uint64, vals []Record) *pbuf.ImportValueRequest {
	columnIDs := make([]uint64, 0, len(vals))
	values := make([]int64, 0, len(vals))
	for _, record := range vals {
		val := record.(FieldValue)
		columnIDs = append(columnIDs, val.ColumnID)
		values = append(values, val.Value)
	}
	return &pbuf.ImportValueRequest{
		Index:     indexName,
		Field:     fieldName,
		Shard:     shard,
		ColumnIDs: columnIDs,
		Values:    values,
	}
}

// ClientOptions control the properties of client connection to the server.
type ClientOptions struct {
	SocketTimeout    time.Duration
	ConnectTimeout   time.Duration
	PoolSizePerRoute int
	TotalPoolSize    int
	TLSConfig        *tls.Config
}

func (co *ClientOptions) addOptions(options ...ClientOption) error {
	for _, option := range options {
		err := option(co)
		if err != nil {
			return err
		}
	}
	return nil
}

// ClientOption is used when creating a PilosaClient struct.
type ClientOption func(options *ClientOptions) error

// OptClientSocketTimeout is the maximum idle socket time in nanoseconds
func OptClientSocketTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.SocketTimeout = timeout
		return nil
	}
}

// OptClientConnectTimeout is the maximum time to connect in nanoseconds.
func OptClientConnectTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.ConnectTimeout = timeout
		return nil
	}
}

// OptPoolSizePerRoute is the maximum number of active connections in the pool to a host.
func OptClientPoolSizePerRoute(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.PoolSizePerRoute = size
		return nil
	}
}

// OptClientTotalPoolSize is the maximum number of connections in the pool.
func OptClientTotalPoolSize(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.TotalPoolSize = size
		return nil
	}
}

// OptClientTLSConfig contains the TLS configuration.
func OptClientTLSConfig(config *tls.Config) ClientOption {
	return func(options *ClientOptions) error {
		options.TLSConfig = config
		return nil
	}
}

type versionInfo struct {
	Version string `json:"version"`
}

func (co *ClientOptions) withDefaults() (updated *ClientOptions) {
	// copy options so the original is not updated
	updated = &ClientOptions{}
	*updated = *co
	// impose defaults
	if updated.SocketTimeout <= 0 {
		updated.SocketTimeout = time.Second * 300
	}
	if updated.ConnectTimeout <= 0 {
		updated.ConnectTimeout = time.Second * 60
	}
	if updated.PoolSizePerRoute <= 0 {
		updated.PoolSizePerRoute = 10
	}
	if updated.TotalPoolSize <= 0 {
		updated.TotalPoolSize = 100
	}
	if updated.TLSConfig == nil {
		updated.TLSConfig = &tls.Config{}
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Shards restricts query to a subset of shards. Queries all shards if nil.
	Shards []uint64
	// ColumnAttrs enables returning columns in the query response.
	ColumnAttrs bool
	// ExcludeRowAttrs inhibits returning attributes
	ExcludeRowAttrs bool
	// ExcludeColumns inhibits returning columns
	ExcludeColumns bool
}

func (qo *QueryOptions) addOptions(options ...interface{}) error {
	for i, option := range options {
		switch o := option.(type) {
		case nil:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			continue
		case *QueryOptions:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			*qo = *o
		case QueryOption:
			err := o(qo)
			if err != nil {
				return err
			}
		default:
			return ErrInvalidQueryOption
		}
	}
	return nil
}

// QueryOption is used when using options with a client.Query,
type QueryOption func(options *QueryOptions) error

// OptQueryColumnAttrs enables returning column attributes in the result.
func OptQueryColumnAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ColumnAttrs = enable
		return nil
	}
}

// OptQueryShards restricts the set of shards on which a query operates.
func OptQueryShards(shards ...uint64) QueryOption {
	return func(options *QueryOptions) error {
		options.Shards = append(options.Shards, shards...)
		return nil
	}
}

// OptQueryExcludeAttrs enables discarding attributes from a result,
func OptQueryExcludeAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeRowAttrs = enable
		return nil
	}
}

// OptQueryExcludeColumns enables discarding columns from a result,
func OptQueryExcludeColumns(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeColumns = enable
		return nil
	}
}

type ImportWorkerStrategy int

const (
	DefaultImport ImportWorkerStrategy = iota
	BatchImport
	TimeoutImport
)

type ImportOptions struct {
	threadCount           int
	shardWidth            uint64
	timeout               time.Duration
	batchSize             int
	strategy              ImportWorkerStrategy
	statusChan            chan<- ImportStatusUpdate
	importRecordsFunction func(indexName string, fieldName string, shard uint64, records []Record) error
}

func (opt *ImportOptions) withDefaults() (updated ImportOptions) {
	updated = *opt
	updated.shardWidth = 1048576

	if updated.threadCount <= 0 {
		updated.threadCount = 1
	}
	if updated.timeout <= 0 {
		updated.timeout = 100 * time.Millisecond
	}
	if updated.batchSize <= 0 {
		updated.batchSize = 100000
	}
	if updated.strategy == DefaultImport {
		updated.strategy = BatchImport
	}
	return
}

// ImportOption is used when running imports.
type ImportOption func(options *ImportOptions) error

func OptImportThreadCount(count int) ImportOption {
	return func(options *ImportOptions) error {
		options.threadCount = count
		return nil
	}
}

func OptImportTimeout(timeout time.Duration) ImportOption {
	return func(options *ImportOptions) error {
		options.timeout = timeout
		return nil
	}
}

func OptImportBatchSize(batchSize int) ImportOption {
	return func(options *ImportOptions) error {
		options.batchSize = batchSize
		return nil
	}
}

func OptImportStrategy(strategy ImportWorkerStrategy) ImportOption {
	return func(options *ImportOptions) error {
		options.strategy = strategy
		return nil
	}
}

func OptImportStatusChannel(statusChan chan<- ImportStatusUpdate) ImportOption {
	return func(options *ImportOptions) error {
		options.statusChan = statusChan
		return nil
	}
}

func importRecordsFunction(fun func(indexName string, fieldName string, shard uint64, records []Record) error) ImportOption {
	return func(options *ImportOptions) error {
		options.importRecordsFunction = fun
		return nil
	}
}

type fragmentNodeRoot struct {
	URI fragmentNode `json:"uri"`
}

type fragmentNode struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

// Status contains the status information from a Pilosa server.
type Status struct {
	Nodes         []StatusNode `json:"nodes"`
	indexMaxShard map[string]uint64
}

// StatusNode contains node information.
type StatusNode struct {
	Scheme  string        `json:"scheme"`
	Host    string        `json:"host"`
	Port    int           `json:"port"`
	Indexes []StatusIndex `json:"indexes"`
}

type SchemaInfo struct {
	Indexes []StatusIndex `json:"indexes"`
}

// StatusIndex contains index information.
type StatusIndex struct {
	Name    string        `json:"name"`
	Options StatusOptions `json:"options"`
	Fields  []StatusField `json:"fields"`
	Shards  []uint64      `json:"shards"`
}

// StatusField contains field information.
type StatusField struct {
	Name    string        `json:"name"`
	Options StatusOptions `json:"options"`
}

// StatusOptions contains options for a field or an index.
type StatusOptions struct {
	FieldType   FieldType `json:"type"`
	CacheType   string    `json:"cacheType"`
	CacheSize   uint      `json:"cacheSize"`
	TimeQuantum string    `json:"timeQuantum"`
	Min         int64     `json:"min"`
	Max         int64     `json:"max"`
	Keys        bool      `json:"keys"`
}

func (so StatusOptions) asIndexOptions() *IndexOptions {
	return &IndexOptions{
		keys: so.Keys,
	}
}

func (so StatusOptions) asFieldOptions() *FieldOptions {
	return &FieldOptions{
		fieldType:   so.FieldType,
		cacheSize:   int(so.CacheSize),
		cacheType:   CacheType(so.CacheType),
		timeQuantum: TimeQuantum(so.TimeQuantum),
		min:         so.Min,
		max:         so.Max,
	}
}

type exportReader struct {
	client       *Client
	shardURIs    map[uint64]*URI
	field        *Field
	body         []byte
	bodyIndex    int
	currentShard uint64
	shardCount   uint64
}

func newExportReader(client *Client, shardURIs map[uint64]*URI, field *Field) *exportReader {
	return &exportReader{
		client:     client,
		shardURIs:  shardURIs,
		field:      field,
		shardCount: uint64(len(shardURIs)),
	}
}

// Read updates the passed array with the exported CSV data and returns the number of bytes read
func (r *exportReader) Read(p []byte) (n int, err error) {
	if r.currentShard >= r.shardCount {
		err = io.EOF
		return
	}
	if r.body == nil {
		uri, _ := r.shardURIs[r.currentShard]
		headers := map[string]string{
			"Accept": "text/csv",
		}
		path := fmt.Sprintf("/export?index=%s&field=%s&shard=%d",
			r.field.index.Name(), r.field.Name(), r.currentShard)
		resp, err := r.client.doRequest(uri, "GET", path, headers, nil)
		if err = anyError(resp, err); err != nil {
			return 0, errors.Wrap(err, "doing export request")
		}
		defer resp.Body.Close()
		r.body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, errors.Wrap(err, "reading response body")
		}
		r.bodyIndex = 0
	}
	n = copy(p, r.body[r.bodyIndex:])
	r.bodyIndex += n
	if n >= len(r.body) {
		r.body = nil
		r.currentShard++
	}
	return
}
