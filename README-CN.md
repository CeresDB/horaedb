![HoraeDB](docs/logo/horaedb-banner-white-small.jpg)

![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![CI](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/incubator-horaedb/actions/workflows/ci.yml)
[![OpenIssue](https://img.shields.io/github/issues/apache/horaedb)](https://github.com/apache/horaedb/issues)
<!-- [![Docker](https://img.shields.io/docker/v/apache/horaedb-server?logo=docker)](https://hub.docker.com/r/apache/horaedb-server) TODO need to wait for first apache version release. --> 

[English](./README.md)

HoraeDB 是一款高性能、分布式的云原生时序数据库。

## 文档
- [用户文档](https://horaedb.apache.org)
- [研发文档](https://horaedb.apache.org/dev/compile_run.html)
- [Roadmap](https://horaedb.apache.org/dev/roadmap.html)

## 快速开始
### 通过 Docker 运行
#### 使用 Docker 运行单机版 HoraeDB
```
docker run -d --name horaedb-server \
  -p 8831:8831 \
  -p 3307:3307 \
  -p 5440:5440 \
  ghcr.io/apache/horaedb-server:nightly-20231222-f57b3827
```

#### 使用 docker compose 运行集群，包含两个 horaedb 节点和一个 horaemeta 节点

```
docker compose -f docker/docker-compose.yaml up
```

### 通过源码编译运行
详见[文档](https://horaedb.apache.org/dev/compile_run.html)。

### 基本操作

创建表
```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
CREATE TABLE `demo` (
    `name` string TAG,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
ENGINE=Analytic
  with
(enable_ttl="false")
'
```

数据写入
```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
INSERT INTO demo (t, name, value)
    VALUES (1702224000000, "horaedb", 100)
'
```

数据查询
```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
SELECT * FROM `demo`
'
```

删除表

```
curl --location --request POST 'http://127.0.0.1:5440/sql' \
-d '
Drop TABLE `demo`
'
```

## 如何贡献

[订阅邮箱参与讨论](mailto:dev-subscribe@horaedb.apache.org) ([订阅](mailto:dev-subscribe@horaedb.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [取消订阅](mailto:dev-unsubscribe@horaedb.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [查看邮件历史记录](https://lists.apache.org/list.html?dev@horaedb.apache.org))


发送 [请求](mailto:dev@horaedb.apache.org?subject=(Request%to%20join%20HoraeDB%20slack)) 至 `dev@horaedb.apache.org` 加入HoraedDB Slack

[约定式提交](https://apache.github.io/incubator-horaedb-docs/cn/dev/conventional_commit)

[如何参与 HoraeDB 代码贡献](CONTRIBUTING.md)


## 致谢
在开发 HoraeDB 的过程中， 我们受到很多开源项目的影响和启发，例如  [influxdb_iox](https://github.com/influxdata/influxdb/tree/main/influxdb_iox), [tikv](https://github.com/tikv/tikv) 等等。感谢这些杰出的项目。

在生产环境中，我们重度使用 [OceanBase](https://github.com/oceanbase/oceanbase) 作为 WAL 和 ObjectStore 的实现，而且 OceanBase 团队还帮助我们一起维护集群的稳定，感谢 OceanBase 团队一直以来的帮助。

## 开源许可
HoraeDB 基于 [Apache License 2.0](LICENSE) 协议。
