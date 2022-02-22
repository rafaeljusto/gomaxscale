# gomaxscale

[![Go Reference](https://pkg.go.dev/badge/github.com/rafaeljusto/gomaxscale/v2.svg)](https://pkg.go.dev/github.com/rafaeljusto/gomaxscale/v2)
[![license](http://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/rafaeljusto/gomaxscale/master/LICENSE)

Go library that allows consuming from [MaxScale](https://mariadb.com/kb/en/maxscale/)
CDC listener. Useful for detecting database changes via binlog.

This consumer follows the connection protocol defined by MaxScale 6
[here](https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/).

## Testing with Docker

For this test environment the following file structure was used:

* 📂 `mariadb-config`
  - 📄 `mariadb.cnf`
* 📂 `mariadb-init`
  - 📄 `00_schema.sql`
* 📂 `maxscale-config`
  - 📄 `maxscale.cnf`
* 📄 `docker-compose.yml`
* 📄 `consumer.go`

### mariadb.cnf

We need to enable replication in the MariaDB master database:
```dosini
[mysqld]
server_id=1
binlog_format=row
binlog_row_image=full
log-bin=/var/log/mysql/mariadb-bin
```

### 00_schema.sql

A basic schema adding the MaxScale user, and some testing database to play with:

```sql
RESET MASTER;

CREATE USER 'maxuser'@'%' IDENTIFIED BY 'maxpwd';
GRANT REPLICATION SLAVE ON *.* TO 'maxuser'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'maxuser'@'%';
GRANT SELECT ON mysql.user TO 'maxuser'@'%';
GRANT SELECT ON mysql.db TO 'maxuser'@'%';
GRANT SELECT ON mysql.tables_priv TO 'maxuser'@'%';
GRANT SELECT ON mysql.columns_priv TO 'maxuser'@'%';
GRANT SELECT ON mysql.procs_priv TO 'maxuser'@'%';
GRANT SELECT ON mysql.proxies_priv TO 'maxuser'@'%';
GRANT SELECT ON mysql.roles_mapping TO 'maxuser'@'%';
GRANT SHOW DATABASES ON *.* TO 'maxuser'@'%';

DROP DATABASE IF EXISTS example;
CREATE DATABASE IF NOT EXISTS example;

USE example;

DROP TABLE IF EXISTS users;
CREATE TABLE users (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `email` VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO `users` (`name`, `email`) VALUES ('John Doe', 'john@doe.com');
INSERT INTO `users` (`name`, `email`) VALUES ('Jane Doe', 'jane@doe.com');
```

### maxscale.cnf

MaxScale configuration to configure the [Avro router](https://mariadb.com/kb/en/mariadb-maxscale-6-avrorouter/)
and expose a listener so `gomaxscale` can retrieve the information.

```dosini
[MaxScale]
threads=1
admin_secure_gui=false
threads=auto
admin_host=0.0.0.0

[server1]
type=server
address=db
port=3306
protocol=MariaDBBackend

[cdc-service]
type=service
router=avrorouter
servers=server1
user=maxuser
password=maxpwd

[cdc-listener]
type=listener
service=cdc-service
protocol=CDC
port=4001

[MariaDB-Monitor]
type=monitor
module=mariadbmon
servers=server1
user=maxuser
password=maxpwd
monitor_interval=5000
```

### docker-compose.yml

To setup a MariaDB database and a MaxScale server we will use docker-compose
with the following configuration:

```yaml
version: '2.4'
services:
  db:
    container_name: "lab-db"
    image: mariadb:10.3.8
    volumes:
      - ./mariadb-config:/etc/mysql/conf.d
      - ./mariadb-init:/docker-entrypoint-initdb.d
    environment:
      MYSQL_ROOT_PASSWORD: abc123
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "--silent"]

  dbproxy:
    container_name: "lab-maxscale"
    image: mariadb/maxscale:6.2
    volumes:
      - ./maxscale-config/maxscale.cnf:/etc/maxscale.cnf
    ports:
      - 4001:4001
    depends_on:
      - db
```

### consumer.go

A local Go file will consume and log the modified items in the database:

```go
package main

import (
  "fmt"
  "log"
  "os"
  "os/signal"
  "syscall"

  "github.com/rafaeljusto/gomaxscale/v2"
)

func main() {
  consumer := gomaxscale.NewConsumer("127.0.0.1:4001", "example", "users",
    gomaxscale.WithAuth("maxuser", "maxpwd"),
  )
  err := consumer.Start()
  if err != nil {
    log.Fatal(err)
  }
  defer consumer.Close()

  fmt.Println("start consuming events")

  done := make(chan bool)
  go func() {
    consumer.Process(func(event gomaxscale.CDCEvent) {
      switch e := event.(type) {
      case gomaxscale.DDLEvent:
        fmt.Printf("ddl event detected on database '%s' and table '%s'\n",
          e.Database, e.Table)
      case gomaxscale.DMLEvent:
        fmt.Printf("dml '%s' event detected\n", e.Type)
      }
    })
    done <- true
  }()

  signalChanel := make(chan os.Signal, 1)
  signal.Notify(signalChanel, syscall.SIGINT, syscall.SIGTERM)

  select {
  case <-signalChanel:
  case <-done:
  }

  fmt.Println("terminating")
}
```

### Running

First, start all services:
```
% docker-compose up -d
```

Then we can start consuming the items:
```
% go run consumer.go
```

To see the magic happening you could do some database changes:
```
% docker-compose exec db mysql -u root -p abc123 -D example \
  -e "INSERT INTO users (name, email) VALUES ('James Doe', 'james@doe.com')"
```