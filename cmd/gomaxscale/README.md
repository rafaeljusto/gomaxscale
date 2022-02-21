# gomaxscale

This is a simple CLI to demonstrate how to use the library. It will print to the
standard output database events.

Example of how to run the program:
```
$ go run main.go \
  -database-name example \
  -database-table users \
  -maxscale-user maxuser \
  -maxscale-password maxpwd
```

The configuration is also available via environment variables:
```
$ GOMAXSCALE_DATABASE_NAME=example \
  GOMAXSCALE_DATABASE_TABLE=users \
  GOMAXSCALE_MAXSCALE_USER=maxuser \
  GOMAXSCALE_MAXSCALE_PASSWORD=maxpwd \
  go run main.go
```