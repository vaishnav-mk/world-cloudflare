CREATE TABLE `start_hook_refusals` (
	`run_id` text PRIMARY KEY NOT NULL,
	`token` text NOT NULL,
	`conflicting_run_id` text NOT NULL
);
