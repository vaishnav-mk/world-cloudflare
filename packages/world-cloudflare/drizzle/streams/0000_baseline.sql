-- Edited to IF NOT EXISTS: Durable Objects created before Drizzle already have these tables.
CREATE TABLE IF NOT EXISTS `chunks` (
	`name` text NOT NULL,
	`position` integer NOT NULL,
	`bytes` blob NOT NULL,
	PRIMARY KEY(`name`, `position`)
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `streams` (
	`name` text PRIMARY KEY NOT NULL,
	`tail` integer DEFAULT -1 NOT NULL,
	`closed` integer DEFAULT 0 NOT NULL
);
