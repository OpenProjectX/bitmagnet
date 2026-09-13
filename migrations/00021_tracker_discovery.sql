-- +goose Up
-- +goose StatementBegin
CREATE TABLE torrent_discovery_tombstones (
 info_hash bytea PRIMARY KEY CHECK (octet_length(info_hash)=20),
 deleted_at timestamptz NOT NULL DEFAULT now()
);
-- Serialize automatic metadata writes and deletes; no network IO holds this lock.
CREATE FUNCTION lock_torrent_discovery_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_advisory_xact_lock(72619401); RETURN NULL; END $$;
CREATE TRIGGER torrent_discovery_delete_lock BEFORE DELETE ON torrents
 FOR EACH STATEMENT EXECUTE FUNCTION lock_torrent_discovery_delete();
CREATE FUNCTION tombstone_torrent_discovery_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO torrent_discovery_tombstones(info_hash) VALUES (OLD.info_hash)
 ON CONFLICT(info_hash) DO UPDATE SET deleted_at=now();
 RETURN OLD;
END $$;
CREATE TRIGGER torrent_discovery_delete AFTER DELETE ON torrents
 FOR EACH ROW EXECUTE FUNCTION tombstone_torrent_discovery_delete();

CREATE TABLE tracker_endpoints (
 id text PRIMARY KEY, announce_url text NOT NULL, scrape_url text NOT NULL,
 next_run timestamptz NOT NULL DEFAULT now(), lease_token text, lease_until timestamptz,
 capability text NOT NULL DEFAULT 'unknown', failures integer NOT NULL DEFAULT 0,
 last_error text, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE tracker_announce_limits (
 group_id text PRIMARY KEY, lease_token text NOT NULL, available_at timestamptz NOT NULL,
 lease_until timestamptz NOT NULL
);
CREATE TABLE tracker_scrape_runs (
 id bigserial PRIMARY KEY, tracker_id text NOT NULL REFERENCES tracker_endpoints(id),
 started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 status text NOT NULL DEFAULT 'running', entries integer NOT NULL DEFAULT 0,
 admitted integer NOT NULL DEFAULT 0, wire_bytes bigint NOT NULL DEFAULT 0,
 decoded_bytes bigint NOT NULL DEFAULT 0, error text
);
CREATE TABLE tracker_hash_observations (
 tracker_id text NOT NULL REFERENCES tracker_endpoints(id),
 info_hash bytea NOT NULL CHECK(octet_length(info_hash)=20),
 seeders bigint NOT NULL, leechers bigint NOT NULL, downloaded bigint NOT NULL,
 first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tracker_id,info_hash)
);
CREATE INDEX tracker_hash_observations_hash ON tracker_hash_observations(info_hash);
CREATE INDEX tracker_hash_observations_seen ON tracker_hash_observations(last_seen);
CREATE TABLE metadata_resolution_work (
 info_hash bytea PRIMARY KEY CHECK(octet_length(info_hash)=20),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','resolved','retry_wait','exhausted','blocked')),
 attempts integer NOT NULL DEFAULT 0, next_attempt timestamptz NOT NULL DEFAULT now(),
 lease_token text, lease_until timestamptz, last_error text, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX metadata_resolution_work_due ON metadata_resolution_work(state,next_attempt);
CREATE INDEX metadata_resolution_work_lease ON metadata_resolution_work(lease_until) WHERE state='leased';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE tracker_announce_limits, metadata_resolution_work, tracker_hash_observations, tracker_scrape_runs, tracker_endpoints;
DROP TRIGGER torrent_discovery_delete ON torrents;
DROP TRIGGER torrent_discovery_delete_lock ON torrents;
DROP FUNCTION tombstone_torrent_discovery_delete();
DROP FUNCTION lock_torrent_discovery_delete();
DROP TABLE torrent_discovery_tombstones;
-- +goose StatementEnd
