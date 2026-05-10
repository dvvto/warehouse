CREATE KEYSPACE IF NOT EXISTS warehouse
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id text,
    zone_id text,
    sku text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY (product_id, zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id text,
    zone_id text,
    sku text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY (product_id, zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id text,
    product_id text,
    sku text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY (zone_id, product_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY,
    status text,
    created_at timestamp,
    updated_at timestamp,
    items text
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    event_type text,
    processed_at timestamp
);
