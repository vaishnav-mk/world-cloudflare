-- Edited to IF NOT EXISTS: Durable Objects created before Drizzle already have these tables.
CREATE TABLE IF NOT EXISTS `dispatch_outbox` (
	`run_id` text NOT NULL,
	`slot` integer NOT NULL,
	`queue_name` text NOT NULL,
	`deployment_id` text NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`redrive` integer DEFAULT 0 NOT NULL,
	`next_at` integer NOT NULL,
	`status` text DEFAULT 'pending' NOT NULL,
	PRIMARY KEY(`run_id`, `slot`)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `dispatch_outbox_pending` ON `dispatch_outbox` (`status`,`next_at`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `events` (
	`run_id` text NOT NULL,
	`slot` integer NOT NULL,
	`kind` text NOT NULL,
	`correlation_id` text,
	`spec_version` integer NOT NULL,
	`created_at` integer NOT NULL,
	`occurred_at` integer,
	`resume_id` text,
	`resume_digest` text,
	`data` text,
	`payload` blob,
	PRIMARY KEY(`run_id`, `slot`)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `events_correlation` ON `events` (`run_id`,`correlation_id`,`slot`);--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `events_resume` ON `events` (`run_id`,`resume_id`) WHERE resume_id IS NOT NULL;--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `hooks` (
	`id` text PRIMARY KEY NOT NULL,
	`run_id` text NOT NULL,
	`token` text NOT NULL,
	`data` text NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS `hooks_token_unique` ON `hooks` (`token`);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `hooks_created` ON `hooks` (`run_id`,`created_at`,`id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `runs` (
	`id` text PRIMARY KEY NOT NULL,
	`data` text NOT NULL,
	`created_at` integer NOT NULL,
	`status` text NOT NULL,
	`workflow_name` text NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `runs_created` ON `runs` (`created_at`,`id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `steps` (
	`run_id` text NOT NULL,
	`id` text NOT NULL,
	`data` text NOT NULL,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`run_id`, `id`)
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `steps_created` ON `steps` (`run_id`,`created_at`,`id`);--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `waits` (
	`run_id` text NOT NULL,
	`id` text NOT NULL,
	`data` text NOT NULL,
	PRIMARY KEY(`run_id`, `id`)
);
