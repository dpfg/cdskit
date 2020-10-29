package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/Songmu/prompter"
	"github.com/jessevdk/go-flags"
)

// Opts represent all available commands supported by utility
type Opts struct {
	DeleteAllCmd DeleteAllCmd `command:"delete-all"`
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
