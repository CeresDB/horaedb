DROP TABLE IF EXISTS `05_create_tables_t`;
DROP TABLE IF EXISTS `05_create_tables_t2`;
DROP TABLE IF EXISTS `05_create_tables_t3`;
DROP TABLE IF EXISTS `05_create_tables_t4`;
DROP TABLE IF EXISTS `05_create_tables_t5`;
DROP TABLE IF EXISTS `05_create_tables_t6`;
DROP TABLE IF EXISTS `05_create_tables_t7`;

-- no TIMESTAMP column
CREATE TABLE `05_create_tables_t`(c1 int) ENGINE = Analytic;

-- TIMESTAMP column doesn't have NOT NULL constrain
CREATE TABLE `05_create_tables_t`(c1 int, t timestamp, TIMESTAMP KEY(t)) ENGINE = Analytic;

CREATE TABLE `05_create_tables_t`(c1 int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

CREATE TABLE IF NOT EXISTS `05_create_tables_t`(c1 int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

-- table already exist
CREATE TABLE `05_create_tables_t`(c1 int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

create table `05_create_tables_t2`(a int, b int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic with (enable_ttl='false');
insert into `05_create_tables_t2`(a, b, t) values(1,1,1),(2,2,2);
select a+b from `05_create_tables_t2`;

-- table already exist
create table `05_create_tables_t2`(a int,b int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
-- table already exist
create table `05_create_tables_t2`(a int,b int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

create table `05_create_tables_t3`(a int,b int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;

create table `05_create_tables_t4`(`a` int, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
describe table `05_create_tables_t4`;
show create table `05_create_tables_t4`;

-- TIMESTAMP KEY
CREATE TABLE `05_create_tables_t5`(c1 int, t timestamp NOT NULL TIMESTAMP KEY) ENGINE = Analytic;
describe table `05_create_tables_t5`;
show create table `05_create_tables_t5`;

-- Multiple TIMESTAMP KEYs
CREATE TABLE `05_create_tables_t6`(c1 int, t1 timestamp NOT NULL TIMESTAMP KEY, t2 timestamp NOT NULL TIMESTAMP KEY) ENGINE = Analytic;

-- Column with comment
CREATE TABLE `05_create_tables_t7`(c1 int COMMENT 'id', t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE = Analytic;
describe table `05_create_tables_t7`;
show create table `05_create_tables_t7`;

DROP TABLE IF EXISTS `05_create_tables_t`;
DROP TABLE IF EXISTS `05_create_tables_t2`;
DROP TABLE IF EXISTS `05_create_tables_t3`;
DROP TABLE IF EXISTS `05_create_tables_t4`;
DROP TABLE IF EXISTS `05_create_tables_t5`;
DROP TABLE IF EXISTS `05_create_tables_t6`;
DROP TABLE IF EXISTS `05_create_tables_t7`;
