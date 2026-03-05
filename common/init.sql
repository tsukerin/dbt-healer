CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE meta.ids (
	tid varchar(255) NULL,
	CONSTRAINT ids_tid_key UNIQUE (tid)
);