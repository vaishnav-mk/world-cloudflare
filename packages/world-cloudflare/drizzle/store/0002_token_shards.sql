CREATE TABLE `token_releases` (
	`hook_id` text PRIMARY KEY NOT NULL,
	`token` text NOT NULL,
	`disposed` integer NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `tokens` (
	`token` text PRIMARY KEY NOT NULL,
	`hook_id` text NOT NULL,
	`data` text NOT NULL,
	`finished` integer DEFAULT 0 NOT NULL
);
