import os.path

from dataclasses import dataclass

from deephaven import dtypes
from deephaven.column import Column, ColumnType
from deephaven.table import Table, PartitionedTable
from deephaven.stream.kafka.consumer import (
    consume_to_partitioned_table,
    ALL_PARTITIONS_SEEK_TO_BEGINNING,
    simple_spec,
    TableType,
    KeyValueSpec,
)
from deephaven.updateby import cum_max
from deephaven.table_listener import listen, TableListenerHandle
from deephaven import parquet
from deephaven.execution_context import make_user_exec_ctx
from deephaven.numpy import to_numpy
from deephaven.parquet import ParquetFileLayout


def get_starting_offsets(historical) -> dict[int, int]:
    latest = (
        historical.view(["KafkaPartition", "KafkaOffset"])
        # Is this efficient, even without Date filter?
        .last_by(["KafkaPartition"])
    )
    kafka_partition = to_numpy(latest, ["KafkaPartition"])[0]
    kafka_offset = to_numpy(latest, ["KafkaOffset"])[0]
    offsets = {}
    for p, o in zip(kafka_partition, kafka_offset):
        # +1 so we get the _next_ offset as starting position
        offsets[p] = o + 1
    return offsets


def read_historical(location: str) -> Table:
    # Note: definition needed here so it can bootstrap when empty
    return parquet.read(
        location,
        file_layout=ParquetFileLayout.KV_PARTITIONED,
        is_refreshing=True,
        table_definition=[
            Column("Date", dtypes.string, column_type=ColumnType.PARTITIONING),
            Column("KafkaPartition", dtypes.int32, column_type=ColumnType.PARTITIONING),
            Column("KafkaOffset", dtypes.int64),
            Column("KafkaTimestamp", dtypes.Instant),
            Column("KafkaKey", dtypes.byte_array),
            Column("KafkaValue", dtypes.byte_array),
        ],
    )


@dataclass
class KafkaToParquetResults:
    historical: PartitionedTable
    live: PartitionedTable
    handler: TableListenerHandle


def kafka_to_parquet(
    location: str,
    kafka_config: dict[str, str],
    topic: str,
    key_spec: KeyValueSpec = simple_spec("KafkaKey", dtypes.byte_array),
    value_spec: KeyValueSpec = simple_spec("KafkaValue", dtypes.byte_array),
) -> KafkaToParquetResults:
    """TODO document"""
    user_exec_ctx = make_user_exec_ctx()

    def add_date(single_partition_table: Table) -> Table:
        """Ensure that we never go backwards with KafkaTimestamp, which is technically possible."""
        with user_exec_ctx:
            return (
                single_partition_table.update_by(
                    [cum_max(["MaxTimestamp = KafkaTimestamp"])]
                )
                .update_view(
                    # need string, not LocalDate, so it works w/ parquet on disk inferred schema by path
                    # ["Date=toLocalDate(MaxTimestamp, java.time.ZoneOffset.UTC)"]
                    ["Date=formatDate(MaxTimestamp, java.time.ZoneOffset.UTC)"]
                )
                .drop_columns(["MaxTimestamp"])
            )

    # Note: definition needed here so it can bootstrap when empty
    historical = read_historical(location)

    # TODO: _THIS_IS_NOT_LIVE TODO
    # https://github.com/deephaven/deephaven-core/issues/4430
    # historical_partitions = historical.select_distinct(["Date", "KafkaPartition"])
    historical_partitions = historical.first_by(["Date", "KafkaPartition"]).view(
        ["Date", "KafkaPartition"]
    )

    offsets = get_starting_offsets(historical)

    kafka_partitions = (
        consume_to_partitioned_table(
            kafka_config,
            topic,
            offsets=offsets,
            key_spec=key_spec,
            value_spec=value_spec,
            # TODO: need to find pattern to keep script up forever; but as of now, append table and partitions grow forever.
            # Script will need occassional restart.
            # https://github.com/deephaven/deephaven-core/pull/4784 might help?
            table_type=TableType.append(),
        ).transform(add_date)
        # TODO: consider adding re_partition_by(...) instead of merge().partition_by(...)
        .merge()
        # Note: unable to use drop_keys=True here, otherwise kafka_partitions.merge() will _not_ include the keys... this should be a bug?
        .partition_by(["Date", "KafkaPartition"])
    )

    # These are the newest partitions; once a new date gets created for KafkaPartition,
    # none of the older ones will receive any more data.
    live_partitions = kafka_partitions.table.view(["Date", "KafkaPartition"]).max_by(
        ["KafkaPartition"]
    )

    # None of these will tick anymore.
    nonlive_partitions = kafka_partitions.table.where_not_in(
        live_partitions, ["Date", "KafkaPartition"]
    )

    def write_to(
        nonlive_partitions: Table,
        path: str,
        overwrite: bool = False,
        log: bool = True,
        compression_codec_name: str = "ZSTD",
    ) -> TableListenerHandle:
        def on_update(update, is_replay):
            added = update.added()
            with user_exec_ctx:
                for date, kafka_partition, constituent in zip(
                    added["Date"], added["KafkaPartition"], added["__CONSTITUENT__"]
                ):
                    location = f"{path}/Date={date}/KafkaPartition={kafka_partition}/table.parquet"
                    if overwrite or not os.path.exists(location):
                        if log:
                            print(f"Writing '{location}'...")
                        # Need to manually drop columns here; see note earlier about not being able to use drop_keys=True
                        parquet.write(
                            Table(constituent).drop_columns(["Date", "KafkaPartition"]),
                            location,
                            compression_codec_name=compression_codec_name,
                        )
                    elif log:
                        print(f"Skipping '{location}'")

        return listen(nonlive_partitions, on_update)

    # it probably makes sense to return the results in some form partitioned for the user...
    live = kafka_partitions.merge().where_not_in(
        historical_partitions, ["Date", "KafkaPartition"]
    )
    handler = write_to(nonlive_partitions, location)
    return KafkaToParquetResults(historical, live, handler)
