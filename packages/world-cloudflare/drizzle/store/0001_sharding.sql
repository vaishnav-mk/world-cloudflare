CREATE TABLE `directory_outbox` (
	`run_id` text PRIMARY KEY NOT NULL,
	`slot` integer NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `directory_releases` (
	`hook_id` text PRIMARY KEY NOT NULL,
	`run_id` text NOT NULL
);
--> statement-breakpoint
ALTER TABLE `runs` ADD `index_slot` integer;