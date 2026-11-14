CREATE TABLE `hook_claim_intents` (
	`run_id` text PRIMARY KEY NOT NULL,
	`operation_id` text NOT NULL,
	`hook_id` text NOT NULL,
	`data` text NOT NULL,
	`next_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `hook_claim_intents_operation_id_unique` ON `hook_claim_intents` (`operation_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `hook_claim_intents_hook_id_unique` ON `hook_claim_intents` (`hook_id`);--> statement-breakpoint
ALTER TABLE `token_takeovers` ADD `operation` text;