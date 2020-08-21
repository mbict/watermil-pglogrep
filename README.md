# Postgres Logical Replication subscriber for watermill

A subsriber to the postgres logical replication WAL. This was created to have a solution to the  2 phase commit problem when storing events in a database and than publish the event.
The Wal is used to make sure that the events are published only when the database has succesfully persisted the event. Also this is one of the most perfomrant way to minimize lateency over other solutions.

But you could use it just for everything ;)

## Run postgres instance
Make sure the following configuration is turned on in the postgres config:
```
wal_level = logical
max_wal_senders = 10                  # minimal 1
max_replication_slots = 10            # minimal 1
```

Start a postgres instance in docker with the following command:
```
docker run --name es-postgres -v "$PWD/my-postgres.conf":/etc/postgresql/postgresql.conf -p 5432:5432 -e POSTGRES_PASSWORD=secret -d postgres -c 'config_file=/etc/postgresql/postgresql.conf'
```

## Configure postgres for replication
Create the database and schema (for this example I used the eventstore tables)
```postgresql
CREATE TABLE events
(
    sequence bigserial not null
        constraint events_pk
            primary key,
    stream_id uuid not null,
    version integer not null,
    type text not null,
    data jsonb,
    metadata jsonb,
    occurred_at timestamp with time zone default now() not null,
    constraint events_pk_unique
        unique (stream_id, version)
);

CREATE INDEX events_stream_id_index ON events (stream_id);

ALTER TABLE events OWNER TO postgres;
```

To get the full change logs and previous states of a row, we need to turn on full identity in the replica 

```postgresql
ALTER TABLE events REPLICA IDENTITY FULL;
```

We next create the replication slot
```postgresql
SELECT pg_create_logical_replication_slot('eventstore_slot', 'pgoutput');
```

And finally we create the subscription, we can specify in what kinda updates we are interested and which tables we want to subscribe to.
```postgresql
--- we only publish inserts on the events table
CREATE PUBLICATION eventstore_cdc FOR TABLE events WITH(publish='insert');

--- we publish all alternations on the events table
CREATE PUBLICATION eventstore_cdc FOR TABLE events;

--- we publish all changes in the database
CREATE PUBLICATION eventstore_cdc FOR ALL TABLES; 
```

## Run NATS instance
```
docker run --rm --entrypoint /nats-streaming-server -p 8222:8222 -p 4222:4222 nats-streaming:0.18 --max_channels=0 -store file -dir datastore
```


 