# Verifiable Credential example

This example creates 10 million Verifiable Credentials. Each credential has a list of resources that may be accessed with the credential. It creates an index on the subject and the resources.

## Running

```shell
go run examples/vcs/main.go 
```

note: it takes a while to populate the DB
