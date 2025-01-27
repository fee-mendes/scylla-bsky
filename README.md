# scylla-bsky

```cql
CREATE KEYSPACE social WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true AND tablets = {'enabled': false};

CREATE TYPE social.post_reply (
    parent text,
    root text
);

CREATE TYPE social.embedding_blob (
    cid text,
    mime_type text,
    size bigint
);

CREATE TYPE social.embed_external (
    description text,
    thumb frozen<embedding_blob>,
    title text,
    uri text
);

CREATE TYPE social.embed_media (
    kind text,
    alt text,
    blob frozen<embedding_blob>,
    aspect_ratio frozen<map<text, int>>
);

CREATE TYPE social.embeddings (
    media frozen<set<frozen<embed_media>>>,
    external frozen<embed_external>,
    record text
);

CREATE TABLE social.likes_by_author (
    author text,
    created_at timestamp,
    cid text,
    subject text,
    PRIMARY KEY (author, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};


CREATE TABLE social.post (
    author text,
    created_at timestamp,
    content text,
    embed embeddings,
    id text,
    labels set<text>,
    language set<text>,
    reply post_reply,
    tags set<text>,
    PRIMARY KEY (author, created_at)
) WITH CLUSTERING ORDER BY (created_at ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};


CREATE TABLE social.post_likes (
    subject text,
    likes counter,
    PRIMARY KEY (subject)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};


CREATE TABLE social.profile (
    did text,
    avatar embedding_blob,
    created_at timestamp,
    description text,
    display_name text,
    labels set<text>,
    pinned_post text,
    PRIMARY KEY (did)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
```
