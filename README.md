## CDSKit - Cloud Datastore Kit

Cloud Datastore Kit - command line utilities to simplify work with Google Cloud Datastore

### Usage

```sh
Usage:
  cdskit [OPTIONS] <delete-all | export-kind>

Help Options:
  -h, --help  Show this help message

Available commands:
  delete-all   Delete all entities
  export-kind  Export all entities to a JSON or CSV

[delete-all command options]
      -p, --project=    Project to be used.
      -n, --namespaces= Namespaces to clean up
      -k, --kinds=      Kinds to clean up

[export-kind command options]
      -p, --project=   Project to be used.
      -n, --namespace= Namespace to get data from
      -k, --kind=      Kind to export
          --format=    One of the follwing formats: csv, json (default: csv)
```
