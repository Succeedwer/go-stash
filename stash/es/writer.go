package es

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"os"
	"strings"

	"github.com/kevwan/go-stash/stash/config"
	"github.com/rogpeppe/go-internal/semver"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

const es8Version = "8.0.0"

type (
	Writer struct {
		docType   string
		esVersion string
		client    *elasticsearch.Client
		inserter  *executors.ChunkExecutor
	}

	valueWithIndex struct {
		index string
		val   string
	}
)

func NewWriter(c config.ElasticSearchConf) (*Writer, error) {

	var cert []byte
	if c.Certs != "" {
		var err error
		cert, err = os.ReadFile(c.Certs)
		if err != nil {
			logx.Errorf("ERROR: Unable to  read CA from %q: %s", c.Certs, err)
		}
	}
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:           c.Hosts,
		Username:            c.Username,
		Password:            c.Password,
		CACert:              cert,
		CompressRequestBody: true,
	})
	logx.Must(err)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	resp, err := client.Cat.Nodes(client.Cat.Nodes.WithV(true))
	if err != nil {
		return nil, err
	}
	version := strings.Split(resp.String(), "\n")[0]
	if !isSupportType(version) {
		logx.Errorf("do not support es version: %s, Only support >= %s", version, es8Version)
		os.Exit(1)
	}

	writer := Writer{
		docType:   c.DocType,
		client:    client,
		esVersion: version,
	}
	writer.inserter = executors.NewChunkExecutor(writer.execute, executors.WithChunkBytes(c.MaxChunkBytes))
	return &writer, nil
}

func (w *Writer) Write(index, val string) error {
	return w.inserter.Add(valueWithIndex{
		index: index,
		val:   val,
	}, len(val))
}

func (w *Writer) execute(vals []interface{}) {

	var buf bytes.Buffer

	for _, val := range vals {
		pair := val.(valueWithIndex)

		m := map[string]map[string]string{
			"index": {"_index": pair.index},
		}
		meta, err := json.Marshal(m)
		if err != nil {
			logx.Errorf("Cannot encode meta: %v", m)
			continue
		}

		data := []byte(pair.val)
		if err != nil {
			logx.Errorf("Cannot encode data: %v", data)
			continue
		}
		meta = append(meta, "\n"...)
		data = append(data, "\n"...)

		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	resp, err := w.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		w.client.Bulk.WithContext(context.Background()),
	)
	logx.Must(err)

	var respMap map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&respMap)
	logx.Must(err)

	if isErr := respMap["errors"].(bool); isErr {
		logx.Error(respMap)
	}
}

func isSupportType(version string) bool {
	// es8.x not support type field
	return semver.Compare(version, es8Version) >= 0
}
