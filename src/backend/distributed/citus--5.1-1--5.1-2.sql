CREATE FUNCTION master_multi_shard_modify(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_multi_shard_modify$$;
COMMENT ON FUNCTION master_multi_shard_modify(text)
    IS 'push delete and update queries to shards';