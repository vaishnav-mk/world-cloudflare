CREATE TABLE `index_hooks` (
	`id` text NOT NULL,
	`run_id` text NOT NULL,
	`data` text NOT NULL,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`id`, `run_id`)
);
--> statement-breakpoint
CREATE INDEX `index_hooks_created` ON `index_hooks` (`created_at`,`id`);--> statement-breakpoint
CREATE TABLE `index_outbox` (
	`run_id` text NOT NULL,
	`hook_id` text NOT NULL,
	`seq` integer DEFAULT 0 NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_at` integer NOT NULL,
	PRIMARY KEY(`run_id`, `hook_id`)
);
--> statement-breakpoint
CREATE INDEX `index_outbox_due` ON `index_outbox` (`next_at`);--> statement-breakpoint
CREATE TABLE `index_runs` (
	`id` text PRIMARY KEY NOT NULL,
	`data` text NOT NULL,
	`created_at` integer NOT NULL,
	`status` text NOT NULL,
	`workflow_name` text NOT NULL
);
--> statement-breakpoint
CREATE INDEX `index_runs_created` ON `index_runs` (`created_at`,`id`);--> statement-breakpoint
CREATE INDEX `index_runs_workflow` ON `index_runs` (`workflow_name`,`created_at`,`id`);--> statement-breakpoint
CREATE INDEX `index_runs_status` ON `index_runs` (`status`,`created_at`,`id`);--> statement-breakpoint
CREATE TABLE `index_sources` (
	`store` text PRIMARY KEY NOT NULL,
	`indexed_at` integer NOT NULL
);
