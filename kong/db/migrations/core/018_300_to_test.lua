return {
    postgres = {
      up = [[
        CREATE TABLE IF NOT EXISTS "cache_entries" (
            "revision"   bigint   NOT NULL,
            "key"        TEXT     NOT NULL,
            "value"      BYTEA    NULL
            );

      ]]
    },
    cassandra = {
    },
  }
