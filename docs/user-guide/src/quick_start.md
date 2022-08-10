# Quick Start

This page shows you how to get started with CeresDB quickly. You'll start a standalone CeresDB server, and then insert and read some sample data using SQL.

## Start server

[CeresDB docker image](https://hub.docker.com/r/ceresdb/ceresdb-server) is the easiest way to get started, if you haven't install Docker, go [there](https://www.docker.com/products/docker-desktop/) to install it first.

You can use command below to start a standalone server
```bash
docker run -d --name ceresdb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  ceresdb/ceresdb-server:v0.2.0
```

CeresDB will listen three ports when start:
- 8831, gRPC port
- 3307, MySQL port
- 5440, HTTP port

The easiest to use is HTTP, so sections below will use it for demo. For production environments, gRPC/MySQL are recommended.

## Write and read data

### Create table
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')"
}'
```

### Write data
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''ceresdb'\'', 100)"
}'
```

### Read data
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "select * from demo"
}'
```

### Show create table
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "show create table demo"
}'
```

### Drop table
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "DROP TABLE demo"
}'
```

## Using the SDKs

See [sdk](./sdk.md)

## Next Step

Congrats, you have finished this tutorial. For more information about CeresDB, see the following:
- [Data Model](./model)
- [Analytic Engine](./analytic_engine)
