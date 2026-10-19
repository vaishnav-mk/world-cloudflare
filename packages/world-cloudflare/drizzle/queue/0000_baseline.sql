-- Edited to IF NOT EXISTS: Durable Objects created before Drizzle already have these tables.
CREATE TABLE IF NOT EXISTS `queue_dedup` (
	`key` text PRIMARY KEY NOT NULL,
	`id` text NOT NULL,
	`queue_name` text NOT NULL,
	`encoded` text,
	`payload_digest` text NOT NULL,
	`delay_ms` integer NOT NULL,
	`target_deployment` text,
	`accepted` integer DEFAULT 0 NOT NULL
);
