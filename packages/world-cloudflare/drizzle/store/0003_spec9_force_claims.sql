CREATE TABLE `forced_disposals` (
	`hook_id` text PRIMARY KEY NOT NULL,
	`run_id` text NOT NULL,
	`token` text NOT NULL,
	`claimed_by_run_id` text NOT NULL,
	`claimed_by_hook_id` text NOT NULL,
	`claimed_from` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `token_takeovers` (
	`token` text PRIMARY KEY NOT NULL,
	`holder` text NOT NULL,
	`claimer` text NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_at` integer NOT NULL
);
--> statement-breakpoint
ALTER TABLE `runs` ADD `seq_in_band` integer;