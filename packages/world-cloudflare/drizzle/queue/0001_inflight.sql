CREATE TABLE `inflight` (
	`id` text PRIMARY KEY NOT NULL,
	`queue_name` text NOT NULL,
	`encoded` text NOT NULL,
	`target_deployment` text
);
