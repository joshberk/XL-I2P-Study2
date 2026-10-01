-- XL-I2P Study 2 schema (MariaDB). Generated from xl_i2p/models.py — do not hand-edit.
-- Create the database first, then run this file:
--   CREATE DATABASE xl_i2p_study2 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

SET NAMES utf8mb4;


CREATE TABLE IF NOT EXISTS epochs (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	label VARCHAR(128) NOT NULL, 
	status VARCHAR(16) NOT NULL, 
	started_at DATETIME NOT NULL, 
	ended_at DATETIME, 
	config_json TEXT, 
	note TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_epochs_label UNIQUE (label)
);


CREATE TABLE IF NOT EXISTS heartbeats (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	updated_at DATETIME NOT NULL, 
	pid INTEGER NOT NULL, 
	hostname VARCHAR(255) NOT NULL, 
	epoch_label VARCHAR(128), 
	phase VARCHAR(64), 
	counters_json TEXT, 
	PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS sites (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	host VARCHAR(255) NOT NULL, 
	base_url VARCHAR(512) NOT NULL, 
	site_type VARCHAR(32) NOT NULL, 
	state VARCHAR(32) NOT NULL, 
	source VARCHAR(128), 
	discovery_method VARCHAR(128), 
	is_cross_layer BOOL NOT NULL, 
	cross_layer_validated_at DATETIME, 
	network_source VARCHAR(128), 
	last_network_observed_at DATETIME, 
	first_seen_at DATETIME NOT NULL, 
	last_seen_at DATETIME, 
	last_checked_at DATETIME, 
	last_crawled_at DATETIME, 
	failure_count INTEGER NOT NULL, 
	success_count INTEGER NOT NULL, 
	next_retry_at DATETIME, 
	last_error_type VARCHAR(64), 
	last_error_message TEXT, 
	created_at DATETIME NOT NULL, 
	updated_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_sites_host UNIQUE (host)
);


CREATE TABLE IF NOT EXISTS crawl_attempts (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	site_id INTEGER NOT NULL, 
	epoch_id INTEGER, 
	attempt_type VARCHAR(32) NOT NULL, 
	started_at DATETIME NOT NULL, 
	finished_at DATETIME, 
	status VARCHAR(32) NOT NULL, 
	discovery_source VARCHAR(128), 
	is_cross_layer BOOL NOT NULL, 
	pages_fetched INTEGER NOT NULL, 
	links_found INTEGER NOT NULL, 
	error_type VARCHAR(64), 
	error_message TEXT, 
	PRIMARY KEY (id), 
	FOREIGN KEY(site_id) REFERENCES sites (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id)
);


CREATE TABLE IF NOT EXISTS cross_layer_observations (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	site_id INTEGER, 
	host VARCHAR(255) NOT NULL, 
	epoch_id INTEGER, 
	input_value VARCHAR(2048), 
	canonical_b32 VARCHAR(255), 
	lookup_method VARCHAR(128) NOT NULL, 
	leaseset_found BOOL NOT NULL, 
	leaseset_hash VARCHAR(512), 
	leaseset_type VARCHAR(64), 
	routing_key VARCHAR(512), 
	published VARCHAR(128), 
	expires VARCHAR(128), 
	gateway_count INTEGER NOT NULL, 
	floodfill_count INTEGER NOT NULL, 
	confidence VARCHAR(128), 
	raw_error TEXT, 
	raw_summary TEXT, 
	console_template_version VARCHAR(128), 
	lease_parser_status VARCHAR(128), 
	observed_at DATETIME NOT NULL, 
	created_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(site_id) REFERENCES sites (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id)
);


CREATE TABLE IF NOT EXISTS network_observations (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	host VARCHAR(255), 
	router_hash VARCHAR(255), 
	epoch_id INTEGER, 
	source_type VARCHAR(64) NOT NULL, 
	source_detail VARCHAR(2048), 
	raw_value TEXT, 
	observed_at DATETIME NOT NULL, 
	created_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id)
);


CREATE TABLE IF NOT EXISTS pages (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	site_id INTEGER NOT NULL, 
	epoch_id INTEGER, 
	url VARCHAR(2048) NOT NULL, 
	normalized_url VARCHAR(2048) NOT NULL, 
	depth INTEGER NOT NULL, 
	http_status INTEGER, 
	content_type VARCHAR(255), 
	content_length INTEGER, 
	title VARCHAR(512), 
	text_length INTEGER, 
	word_count INTEGER, 
	content_hash VARCHAR(64), 
	fetched_at DATETIME NOT NULL, 
	response_time_ms INTEGER, 
	error_type VARCHAR(64), 
	error_message TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_pages_url_epoch UNIQUE (normalized_url, epoch_id), 
	FOREIGN KEY(site_id) REFERENCES sites (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id)
);


CREATE TABLE IF NOT EXISTS links (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	source_site_id INTEGER NOT NULL, 
	source_page_id INTEGER, 
	epoch_id INTEGER, 
	target_host VARCHAR(255) NOT NULL, 
	target_url VARCHAR(2048) NOT NULL, 
	target_site_id INTEGER, 
	anchor_text VARCHAR(512), 
	link_type VARCHAR(32) NOT NULL, 
	first_seen_at DATETIME NOT NULL, 
	last_seen_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_links_page_target UNIQUE (source_page_id, target_url), 
	FOREIGN KEY(source_site_id) REFERENCES sites (id), 
	FOREIGN KEY(source_page_id) REFERENCES pages (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id), 
	FOREIGN KEY(target_site_id) REFERENCES sites (id)
);


CREATE TABLE IF NOT EXISTS seed_events (
	id INTEGER NOT NULL AUTO_INCREMENT, 
	host VARCHAR(255) NOT NULL, 
	source_type VARCHAR(64) NOT NULL, 
	source_detail VARCHAR(2048), 
	source_key VARCHAR(64) NOT NULL, 
	discovered_from_site_id INTEGER, 
	discovered_from_page_id INTEGER, 
	epoch_id INTEGER, 
	count INTEGER NOT NULL, 
	first_seen_at DATETIME NOT NULL, 
	last_seen_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_seed_events_dedup UNIQUE (host, source_type, source_key), 
	FOREIGN KEY(discovered_from_site_id) REFERENCES sites (id), 
	FOREIGN KEY(discovered_from_page_id) REFERENCES pages (id), 
	FOREIGN KEY(epoch_id) REFERENCES epochs (id)
);

-- Induced graph views (parity with Study 1 analysis).
CREATE OR REPLACE VIEW v_induced_nodes AS
  SELECT * FROM sites WHERE state = 'CRAWLED';

CREATE OR REPLACE VIEW v_induced_edges AS
  SELECT DISTINCT l.source_site_id, l.target_site_id
  FROM links l
  JOIN sites s1 ON s1.id = l.source_site_id AND s1.state = 'CRAWLED'
  JOIN sites s2 ON s2.id = l.target_site_id AND s2.state = 'CRAWLED'
  WHERE l.target_site_id IS NOT NULL
    AND l.source_site_id <> l.target_site_id;
