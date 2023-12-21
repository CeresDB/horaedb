package main

import (
	"context"
	"fmt"

	"github.com/apache/incubator-horaedb-client-go/horaedb"
)

const fieldName = "b"
const tagName = "btag"
const timestampName = "t"

func checkPartitionTableAddColumn(ctx context.Context, client horaedb.Client) error {
	err := dropTable(ctx, client, partitionTable)
	if err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, fmt.Sprintf(
		"CREATE TABLE `%s`(   "+
			"`name`string TAG,"+
			"`id` int TAG,"+
			"`value` int64 NOT NULL,"+
			"`t` timestamp NOT NULL,"+
			"TIMESTAMP KEY(t)) "+
			"PARTITION BY KEY(name) PARTITIONS 4 ENGINE = Analytic", partitionTable))
	if err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN (%s string);", partitionTable, fieldName))
	if err != nil {
		return err
	}

	ts := currentMS()

	// First write will fail, because the schema is not updated yet.
	// Currently, horaedb.will update the schema when write failed.
	err = writePartitionTableNewField(ctx, client, ts, fieldName)
	if err == nil {
		panic("first write should fail")
	}

	if err := writePartitionTableNewField(ctx, client, ts, fieldName); err != nil {
		return err
	}

	_, err = ddl(ctx, client, partitionTable, fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN (%s string TAG);", partitionTable, tagName))
	if err != nil {
		return err
	}

	// First write will fail, because the schema is not updated yet.
	// Currently, write failed will update the schema.
	err = writePartitionTableNewTag(ctx, client, ts, tagName)
	if err == nil {
		panic("first write should fail")
	}

	if err := writePartitionTableNewTag(ctx, client, ts, tagName); err != nil {
		return err
	}

	if err := queryPartitionTable(ctx, client, ts, timestampName); err != nil {
		return err
	}

	return nil
}

func writePartitionTableNewField(ctx context.Context, client horaedb.Client, ts int64, fieldName string) error {
	points := make([]horaedb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := horaedb.NewPointBuilder(partitionTable).
			SetTimestamp(ts).
			AddTag("name", horaedb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", horaedb.NewInt64Value(int64(i))).
			AddField(fieldName, horaedb.NewStringValue("ss"))

		point, err := builder.Build()

		if err != nil {
			return err
		}
		points = append(points, point)
	}

	resp, err := client.Write(ctx, horaedb.WriteRequest{
		Points: points,
	})
	if err != nil {
		return err
	}

	if resp.Success != 2 {
		return fmt.Errorf("write failed, resp: %+v", resp)
	}
	return nil
}

func writePartitionTableNewTag(ctx context.Context, client horaedb.Client, ts int64, tagName string) error {
	points := make([]horaedb.Point, 0, 2)
	for i := 0; i < 2; i++ {
		builder := horaedb.NewPointBuilder(partitionTable).
			SetTimestamp(ts).
			AddTag("name", horaedb.NewStringValue(fmt.Sprintf("tag-%d", i))).
			AddField("value", horaedb.NewInt64Value(int64(i))).
			AddTag(tagName, horaedb.NewStringValue("sstag")).
			AddField(fieldName, horaedb.NewStringValue("ss"))

		point, err := builder.Build()

		if err != nil {
			return err
		}
		points = append(points, point)
	}

	resp, err := client.Write(ctx, horaedb.WriteRequest{
		Points: points,
	})
	if err != nil {
		return err
	}

	if resp.Success != 2 {
		return fmt.Errorf("write failed, resp: %+v", resp)
	}
	return nil
}

func queryPartitionTable(ctx context.Context, client horaedb.Client, ts int64, timestampName string) error {
	sql := fmt.Sprintf("select t, name, value,%s,%s from %s where %s = %d order by name,%s", fieldName, tagName, partitionTable, timestampName, ts, tagName)

	resp, err := client.SQLQuery(ctx, horaedb.SQLQueryRequest{
		Tables: []string{partitionTable},
		SQL:    sql,
	})
	if err != nil {
		return err
	}

	if len(resp.Rows) != 4 {
		return fmt.Errorf("expect 2 rows, current: %+v", len(resp.Rows))
	}

	row0 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-0"),
		horaedb.NewInt64Value(0),
		horaedb.NewStringValue("ss"),
		horaedb.NewStringValue("sstag"),
	}

	row1 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-0"),
		horaedb.NewInt64Value(0),
		horaedb.NewStringValue("ss"),
	}

	row2 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-1"),
		horaedb.NewInt64Value(1),
		horaedb.NewStringValue("ss"),
		horaedb.NewStringValue("sstag"),
	}

	row3 := []horaedb.Value{
		horaedb.NewInt64Value(ts),
		horaedb.NewStringValue("tag-1"),
		horaedb.NewInt64Value(1),
		horaedb.NewStringValue("ss"),
	}

	if err := ensureRow(row0,
		resp.Rows[0].Columns()); err != nil {
		return err
	}
	if err := ensureRow(row1,
		resp.Rows[1].Columns()); err != nil {
		return err
	}
	if err := ensureRow(row2,
		resp.Rows[2].Columns()); err != nil {
		return err
	}

	return ensureRow(row3, resp.Rows[3].Columns())
}
