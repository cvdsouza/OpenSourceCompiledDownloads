package expr

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/influxdb/influxdb/client"

	"bosun.org/_third_party/github.com/MiniProfiler/go/miniprofiler"
	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/opentsdb"
)

// Request holds query objects. Currently only absolute times are supported.
type idbRequest struct {
	Start *time.Time
	End   *time.Time
	Query string
	URL   *url.URL
}

type IDBContext interface {
	URL() url.URL
}

type iContext url.URL

func (self iContext) URL() url.URL {
	return url.URL(self)
}
func NewIDBContext(u url.URL) iContext {
	return iContext(u)
}

func InfluxDBQuery(e *State, T miniprofiler.Timer, query string, sduration, eduration, format string) (r *Results, err error) {

	fmt.Printf("Starting InfluxDBQuery: \n\tState: %v\n\tQuery: %#v\n", e, query)
	sd, err := opentsdb.ParseDuration(sduration)
	if err != nil {
		return
	}
	ed := opentsdb.Duration(0)
	if eduration != "" {
		ed, err = opentsdb.ParseDuration(eduration)
		if err != nil {
			return
		}
	}
	st := e.now.Add(-time.Duration(sd))
	et := e.now.Add(-time.Duration(ed))
	req := &idbRequest{
		Query: query,
		Start: &st,
		End:   &et,
	}
	r, err = timeInfluxDBRequest(e, T, req)
	if err != nil {
		return nil, err
	}
	// formatTags := strings.Split(format, ".")
	return
}

func influxDBTagQuery(args []parse.Node) (parse.Tags, error) {
	log.Printf("Starting influxDBTagQuery: %v\n", args)
	t := make(parse.Tags)
	n := args[3].(*parse.StringNode)
	for _, s := range strings.Split(n.Text, ".") {
		if s != "" {
			t[s] = struct{}{}
		}
	}
	log.Printf("Returning tags: %v\n", t)
	return t, nil
}

func timeInfluxDBRequest(e *State, T miniprofiler.Timer, req *idbRequest) (r *Results, err error) {
	log.Printf("Query start: %v end %v\n", req.Start.String(), req.End.String())
	// e.graphiteQueries = append(e.graphiteQueries, *req)
	// b, _ := json.MarshalIndent(req, "", "  ")
	T.StepCustomTiming("influxDB", "query", "value here", func() {
		r = new(Results)
		r.Results = make([]*Result, 0)

		hasWhere := strings.Contains(strings.ToLower(req.Query), "where")
		queryString := req.Query
		if !hasWhere {
			queryString = queryString + " where "
		} else {
			queryString = queryString + " and "
		}
		timeFormat := "YYYY-MM-DD HH:MM:SS.mmm"
		timeFormat = "2006-01-02 15:04:05"
		queryString += " time > '" + req.Start.Format(timeFormat) + "' and time < '" + req.End.Format(timeFormat) + "'"

		// Add a group by to get the tags
		queryString += " group by *"

		log.Printf("Executing query: %v\n", queryString)

		connection, _ := client.NewClient(client.Config{URL: e.influxDBContext.URL()})
		q := client.Query{
			Command:  queryString,
			Database: "dewey",
		}
		response, err := connection.Query(q)
		if err != nil {
			log.Printf("Error querying InfluxDB: %v", err)
			return
		}

		log.Printf("Respones from Influx:\n")
		log.Printf("\tResults: %v\n", len(response.Results))

		// Loop over the responses...
		// results := make([]*Result, 0)
		for _, result := range response.Results {
			// tags := make(opentsdb.TagSet)
			log.Printf("\t\tResult had %v Series/Rows\n", len(result.Series))
			for _, row := range result.Series {
				// Create a Result object
				tags := opentsdb.TagSet(row.Tags)
				s := make(Series)
				for _, d := range row.Values {
					if len(d) != 2 {
						r = nil
						err = fmt.Errorf("influxDB ParseError: %s", fmt.Sprintf("Datapoint has more than 2 fields: %v", d))
						return
					}
					timeStampString, tsOK := d[0].(string)
					number, numberOK := d[1].(json.Number)
					log.Printf("Parsing value %v TS: %v Number: %v", d, tsOK, numberOK)
					log.Printf("Found row: %v of %v, %v\n", d, reflect.TypeOf(d[0]), reflect.TypeOf(d[1]))
					if tsOK && numberOK {
						timeStamp, _ := time.Parse(time.RFC3339, timeStampString)
						v, _ := number.Float64()
						s[timeStamp] = v
					}
				}

				log.Printf("Tags: %v\n", tags)
				log.Printf("Columns: %v\n", row.Columns)
				// log.Printf("Data: %v\n", row.Values)
				r.Results = append(r.Results, &Result{Value: s, Group: tags})
			}
		}
		// key := req.CacheKey()
		// getFn := func() (interface{}, error) {
		// 	return e.influxDBContext.Query(req)
		// }
		// var val interface{}
		// val, err = e.cache.Get(key, getFn)
		// // resp = val.(idbResponse)
	})
	return
}
