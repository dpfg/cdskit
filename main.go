package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/Songmu/prompter"
	"github.com/jessevdk/go-flags"
)

// Opts represent all available commands supported by utility
type Opts struct {
	DeleteAllCmd  DeleteAllCmd  `command:"delete-all"`
	ExportKindCmd ExportKindCmd `command:"export-kind"`
}

// DeleteAllCmd is a command to delete all entities inside namespaces and a certain kind of
type DeleteAllCmd struct {
	ProjectID  string `short:"p" long:"project" description:"Project to be used." required:"true"`
	Namespaces string `short:"n" long:"namespaces" description:"Namespaces to clean up"`
	Kinds      string `short:"k" long:"kinds" description:"Kinds to clean up"`
}

// Execute is called by go-flags
func (cmd *DeleteAllCmd) Execute(args []string) error {

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, cmd.ProjectID)
	if err != nil {
		return err
	}

	defer dsClient.Close()

	namespaces := strings.Split(cmd.Namespaces, ",")
	if len(namespaces) == 0 || cmd.Namespaces == "" {

		metadatNS, err := metadataNamespaces(ctx, dsClient)
		if err != nil {
			return fmt.Errorf("Unable to load list of namespaces: %w", err)
		}

		if len(metadatNS) > 0 {
			query := fmt.Sprintf("Entities from the following namespaces will be deleted: %s\n", strings.Join(metadatNS, "\n"))

			var choices []string
			copy(choices, metadatNS)
			choices = append(choices, "all")
			choice := prompter.Choose(query, choices, "all")

			if choice == "all" {
				namespaces = metadatNS
			} else {
				namespaces = []string{choice}
			}
		}
	}

	for _, ns := range namespaces {

		kinds := strings.Split(cmd.Kinds, ",")
		if len(kinds) == 0 || cmd.Kinds == "" {
			kinds, err = metadataKinds(ctx, dsClient, ns)
			if err != nil {
				return err
			}
		}

		for _, kind := range kinds {

			fmt.Printf("Deleting %s/%s ... ", ns, kind)

			keys, err := dsClient.GetAll(ctx, datastore.NewQuery(kind).Namespace(ns).KeysOnly(), nil)
			if err != nil {
				return err
			}

			fmt.Printf("Keys: %d\n", len(keys))

			for i := 0; i < len(keys); i += 500 {
				batch := keys[i:min(i+500, len(keys))]
				err = dsClient.DeleteMulti(ctx, batch)
				if err != nil {
					return err
				}
			}
		}
	}

	fmt.Println("-------------------------------------------------------------------")
	fmt.Println("All entities have been successfully deleted!")
	fmt.Println("Namespaces itself will be cleaned up automatically within 48 hours.")

	return nil
}

func main() {

	var opts Opts
	p := flags.NewParser(&opts, flags.Default)

	if _, err := p.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func metadataNamespaces(ctx context.Context, client *datastore.Client) ([]string, error) {
	query := datastore.NewQuery("__namespace__").KeysOnly()
	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("client.GetAll: %v", err)
	}

	var nss []string
	for _, k := range keys {
		nss = append(nss, k.Name)
	}
	return nss, nil
}

func metadataKinds(ctx context.Context, client *datastore.Client, ns string) ([]string, error) {
	query := datastore.NewQuery("__kind__").Namespace(ns).KeysOnly()
	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("client.GetAll: %v", err)
	}

	var kinds []string
	for _, k := range keys {
		if strings.HasPrefix(k.Name, "__") && strings.HasSuffix(k.Name, "__") {
			continue
		}
		kinds = append(kinds, k.Name)
	}
	return kinds, nil
}

// ExportKindCmd dump kind to a json file
type ExportKindCmd struct {
	ProjectID string `short:"p" long:"project" description:"Project to be used." required:"true"`
	Namespace string `short:"n" long:"namespace" description:"Namespace to get data from"`
	Kind      string `short:"k" long:"kind" description:"Kind to get"`
	Format    string `long:"format" default: "csv" description:"One of the follwing formats: csv, json"`
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

	var r []*DynamicEntity
	keys, err := dsClient.GetAll(ctx, datastore.NewQuery(cmd.Kind).Namespace(cmd.Namespace), &r)
	if err != nil {
		return err
	}

	fmt.Printf("Exporting %d entities...\n", len(keys))

	f, err := os.Create(newName())
	if err != nil {
		return err
	}

	f.WriteString("[")
	first := true
	for _, v := range r {
		b, err := v.Marshal()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to marshal entry: %s", err.Error())
			continue
		}
		if !first {
			f.WriteString(",\n")
		}
		_, err = f.Write(b)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to write entry: %s", err.Error())
			continue
		}
		first = false
	}
	f.WriteString("]")

	return nil
}

func (cmd *ExportKindCmd) newName() string {
	return fmt.Sprintf("export_%s_%s.%s", cmd.Kind, time.Now().Format("2006-01-02T15-04-05Z07-00"), cmd.Format)
}

type DynamicEntity struct {
	value map[string]interface{}
}

// Load loads all of the provided properties into l.
// It does not first reset *l to an empty slice.
func (de *DynamicEntity) Load(ps []datastore.Property) error {
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

// Save saves all of l's properties as a slice of Properties.
func (de *DynamicEntity) Save() ([]datastore.Property, error) {
	return nil, nil
}

// ToJSON converts entry into the JSON
func (de *DynamicEntity) ToJSON() ([]byte, error) {
	return json.Marshal(de.value)
}

// ToCSVHeader converts entry into the encoding/csv consumable array
func (de *DynamicEntity) ToCSVHeader() []string {
	header := make([]string, 0)
	for k, v := range de.value {
		switch val := v.(type) {
		case map[string]interface{}:
			header = append(header, "")
		case []interface{}:
			header = append(header, k)
		default:
			header = append(header, k)
		}
	}
	// return json.Marshal(de.value)
}

// ToCSV converts entry into the encoding/csv consumable array
func (de *DynamicEntity) ToCSVRecord() []string {
	// return json.Marshal(de.value)
	return make([]string, 0)
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
