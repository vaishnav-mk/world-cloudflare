CREATE TABLE `delivery_counts` (
	`id` text PRIMARY KEY NOT NULL,
	`deliveries` integer DEFAULT 0 NOT NULL,
	`handoffs` integer DEFAULT 0 NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX `delivery_counts_updated` ON `delivery_counts` (`updated_at`);