package main

import (
	"context"
	"encoding"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/datastore"
)

// ExportKindCmd dump kind to a json file
type ExportKindCmd struct {
	ProjectID string `short:"p" long:"project" description:"Project to be used." required:"true"`
	Namespace string `short:"n" long:"namespace" description:"Namespace to get data from"`
	Kind      string `short:"k" long:"kind" description:"Kind to export" required:"true"`
	Format    string `long:"format" default:"json" description:"One of the follwing formats: csv, json"`
}

// Execute is called by go-flags
func (cmd *ExportKindCmd) Execute(args []string) error {
	fmt.Fprintf(os.Stderr, "Exporting '%s' from '%s/%s'\n", cmd.Kind, cmd.ProjectID, cmd.Namespace)

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, cmd.ProjectID)
	if err != nil {
		return err
	}

	defer dsClient.Close()

	err = os.MkdirAll(cmd.newExportFolder(), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(cmd.newExportFileName())
	if err != nil {
		return err
	}

	w := cmd.newExportWriter(f)

	read := -1
	offset := 0

	w.WriteHeader()
	for read != 0 {

		q := datastore.NewQuery(cmd.Kind).Namespace(cmd.Namespace).Offset(offset).Limit(1000)

		var batch []*dynamicEntity
		_, err := dsClient.GetAll(ctx, q, &batch)

		if err != nil {
			return err
		}

		read = len(batch)
		if read == 0 {
			continue
		}

		fmt.Fprintf(os.Stderr, "Exporintg %s - %d\n", cmd.Kind, offset+read)

		if offset != 0 {
			// second group, write up line break
			w.WriteLineBreak()
		}

		for i, v := range batch {
			w.WriterRecord(v)

			if i != len(batch) {
				w.WriteLineBreak()
			}
		}

		offset = offset + len(batch)
	}
	w.WriteFooter()

	return nil
}

func (cmd ExportKindCmd) newExportWriter(w io.Writer) exportWriter {
	switch cmd.Format {
	case "csv":
		return &csvExportWriter{csvw: csv.NewWriter(w)}
	case "json":
		return &jsonExportWriter{writer: w}
	default:
		panic("Unsupported format: " + cmd.Format)
	}
}

func (cmd *ExportKindCmd) newExportFolder() string {
	return "exports/"
}

func (cmd *ExportKindCmd) newExportFileName() string {
	return fmt.Sprintf("exports/export_%s_%s.%s", cmd.Kind, time.Now().Format("2006-01-02T15-04-05Z07-00"), cmd.Format)
}

type dynamicEntity struct {
	value map[string]interface{}
}

// Load loads all of the provided properties into l.
// It does not first reset *l to an empty slice.
func (de *dynamicEntity) Load(ps []datastore.Property) error {
	if de.value == nil {
		de.value = make(map[string]interface{})
	}

	for _, p := range ps {
		if p.Value != nil {
			de.value[p.Name] = toExportValue(p)
		}
	}
	return nil
}

// Save is never used just completes interface
func (de *dynamicEntity) Save() ([]datastore.Property, error) {
	return nil, nil
}

// ToJSON converts entry into the JSON
func (de *dynamicEntity) ToJSON() ([]byte, error) {
	return json.Marshal(de.value)
}

// ToCSVHeader converts entry into the encoding/csv consumable array
func (de *dynamicEntity) ToCSVHeader() []string {
	header := make([]string, 0)
	traverse(de.value, func(key string, val interface{}) {
		header = append(header, key)
	})
	return header
}

func traverse(v interface{}, fn func(string, interface{})) {
	switch tv := v.(type) {
	case map[string]interface{}:
		for sk, sv := range tv {
			traverse(sv, func(ssk string, v interface{}) {
				if ssk == "" {
					fn(sk, v)
				} else {
					fn(fmt.Sprintf("%s:%s", sk, ssk), v)
				}
			})
		}
	default:
		fn("", v)
	}
}

// ToCSV converts entry into the encoding/csv consumable array
func (de *dynamicEntity) ToCSVRecord() []string {
	row := make([]string, 0)
	traverse(de.value, func(key string, val interface{}) {
		if tm, ok := val.(encoding.TextMarshaler); ok {
			v, _ := tm.MarshalText()
			row = append(row, string(v))
		} else {
			row = append(row, fmt.Sprintf("%v", val))
		}
	})
	return row
}

func toExportValue(value interface{}) interface{} {
	switch v := value.(type) {
	case *datastore.Entity:
		f := make(map[string]interface{})
		for _, pp := range v.Properties {
			if pp.Value == nil {
				continue
			}
			f[pp.Name] = toExportValue(pp.Value)
		}
		return f
	case *datastore.Key:
		id := v.Name
		if len(id) == 0 {
			id = fmt.Sprint(v.ID)
		}
		return id
	case []interface{}:
		f := make([]interface{}, 0)
		for _, pp := range v {
			if pp == nil {
				continue
			}
			f = append(f, toExportValue(pp))
		}
		return f
	case datastore.Property:
		return toExportValue(v.Value)
	default:
		return value
	}

}

type exportWriter interface {
	WriteHeader()
	WriteLineBreak()
	WriterRecord(de *dynamicEntity)
	WriteFooter()
}

type jsonExportWriter struct {
	writer io.Writer
}

func (format jsonExportWriter) WriteHeader() {
	format.writer.Write([]byte("["))
}

func (format *jsonExportWriter) WriterRecord(de *dynamicEntity) {
	v, err := de.ToJSON()

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to marshal entry: %s", err.Error())
		return
	}

	_, err = format.writer.Write(v)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to write entry: %s", err.Error())
		return
	}

}

func (format *jsonExportWriter) WriteLineBreak() {
	format.writer.Write([]byte(",\n"))
}

func (format jsonExportWriter) WriteFooter() {
	format.writer.Write([]byte("]"))
}

type csvExportWriter struct {
	csvw   *csv.Writer
	headed bool
}

func (format csvExportWriter) WriteHeader() {

}

func (format *csvExportWriter) WriterRecord(de *dynamicEntity) {
	if !format.headed {
		format.csvw.Write(de.ToCSVHeader())
		format.headed = true
	}
	format.csvw.Write(de.ToCSVRecord())
}

func (format *csvExportWriter) WriteLineBreak() {

}

func (format csvExportWriter) WriteFooter() {

}
