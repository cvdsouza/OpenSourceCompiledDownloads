package web

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"bosun.org/cmd/bosun/conf"
)

func TestRelay(t *testing.T) {
	schedule.Init(new(conf.Conf))
	rs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer rs.Close()
	rurl, err := url.Parse(rs.URL)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(Relay(rurl.Host))
	defer ts.Close()

	body := []byte(`[{
		"timestamp": 1,
		"metric": "no-gzip-works",
		"value": 123.45,
		"tags": {
			"host": "host.no.gzip",
			"other": "something"
		}
	}]`)
	if _, err := http.Post(ts.URL, "application/json", bytes.NewBuffer(body)); err != nil {
		t.Fatal(err)
	}

	bodygzip := []byte(`[{
		"timestamp": 1,
		"metric": "gzip-works",
		"value": "345",
		"tags": {
			"host": "host.gzip",
			"gzipped": "yup"
		}
	}]`)
	buf := new(bytes.Buffer)
	gw := gzip.NewWriter(buf)
	gw.Write(bodygzip)
	gw.Flush()
	if _, err := http.Post(ts.URL, "application/json", bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	schedule.Search.Copy()
	m := schedule.Search.UniqueMetrics()
	sort.Strings(m)
	if len(m) != 2 || m[0] != "gzip-works" || m[1] != "no-gzip-works" {
		t.Errorf("bad um: %v", m)
	}
	m = schedule.Search.TagValuesByMetricTagKey("gzip-works", "gzipped", 0)
	if len(m) != 1 || m[0] != "yup" {
		t.Errorf("bad tvbmtk: %v", m)
	}
	m = schedule.Search.TagKeysByMetric("no-gzip-works")
	sort.Strings(m)
	if len(m) != 2 || m[0] != "host" || m[1] != "other" {
		t.Errorf("bad tkbm: %v", m)
	}
}
