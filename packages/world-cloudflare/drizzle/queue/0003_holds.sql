CREATE TABLE `holds` (
	`id` text PRIMARY KEY NOT NULL,
	`lease_until` integer NOT NULL,
	`done` integer DEFAULT 0 NOT NULL,
	`queue_name` text NOT NULL,
	`encoded` text NOT NULL,
	`target_deployment` text
);
