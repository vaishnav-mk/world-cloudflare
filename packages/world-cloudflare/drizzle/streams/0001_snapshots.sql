CREATE TABLE `snapshot` (
	`size` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `snapshot_parts` (
	`part` integer PRIMARY KEY NOT NULL,
	`bytes` blob NOT NULL
);
