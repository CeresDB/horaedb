--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

DROP TABLE IF EXISTS `02_function_aggregate_table1`;

CREATE TABLE `02_function_aggregate_table1` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `value` int,
    `uvalue` uint64,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

INSERT INTO `02_function_aggregate_table1`
    (`timestamp`, `arch`, `datacenter`, `value`, `uvalue`)
VALUES
    (1658304762, 'x86-64', 'china', 100, 10),
    (1658304763, 'x86-64', 'china', 200, 10),
    (1658304762, 'arm64', 'china', 110, 0),
    (1658304763, 'arm64', 'china', 210, 0);


SELECT sum(`value`) FROM `02_function_aggregate_table1`;

SELECT
    `arch`,
    sum(`value`)
FROM
    `02_function_aggregate_table1`
WHERE
    `timestamp` BETWEEN 1658304763 AND 1658304763
GROUP BY
    `arch`
ORDER BY
    `arch` DESC;


SELECT count(`value`) FROM `02_function_aggregate_table1`;

SELECT avg(`value`) FROM `02_function_aggregate_table1`;

SELECT max(`value`) FROM `02_function_aggregate_table1`;

SELECT min(`value`) FROM `02_function_aggregate_table1`;

SELECT min(`uvalue`) - max(`uvalue`) FROM `02_function_aggregate_table1`;

-- duplicate with last insert
INSERT INTO `02_function_aggregate_table1`
    (`timestamp`, `arch`, `datacenter`, `value`)
VALUES
    (1658304762, 'x86-64', 'china', 100);

SELECT count(`arch`) FROM `02_function_aggregate_table1`;

SELECT distinct(`arch`) FROM `02_function_aggregate_table1` ORDER BY `arch` DESC;

SELECT count(distinct(`arch`)) FROM `02_function_aggregate_table1`;

CREATE TABLE `02_function_aggregate_table2` (
    `timestamp` timestamp NOT NULL,
    `arch` string TAG,
    `datacenter` string TAG,
    `value` int,
    `uvalue` uint64,
    timestamp KEY (timestamp)) ENGINE=Analytic
WITH(
	 enable_ttl='false',
     update_mode = 'append'
);

INSERT INTO `02_function_aggregate_table2`
    (`timestamp`, `arch`, `datacenter`, `value`, `uvalue`)
VALUES
    (1658304762, 'x86-64', 'china', 100, 10),
    (1658304763, 'x86-64', 'china', 200, 10),
    (1658304762, 'arm64', 'china', 110, 0),
    (1658304763, 'arm64', 'china', 210, 0);

-- The should select empty column
SELECT count(*) FROM `02_function_aggregate_table1`;

-- Same with before, but query from sst
-- SQLNESS ARG pre_cmd=flush
SELECT count(*) FROM `02_function_aggregate_table1`;

DROP TABLE `02_function_aggregate_table1`;
DROP TABLE `02_function_aggregate_table2`;
