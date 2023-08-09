create table pgxjob_groups (
	id int primary key generated by default as identity,
	name text not null unique
);

create table pgxjob_types (
	id int primary key generated by default as identity,
	name text not null unique
);

create table pgxjob_workers (
	id int primary key,
	inserted_at timestamptz not null default now(),
	heartbeat timestamptz not null
);

create sequence pgxjob_workers_id_seq as int cycle owned by pgxjob_workers.id;
alter table pgxjob_workers alter column id set default nextval('pgxjob_workers_id_seq');

-- pgxjob_jobs can potentially be a very hot table. We want to keep it as small as possible.
--
-- Columns are carefully ordered to avoid wasted space. See https://www.2ndquadrant.com/en/blog/on-rocks-and-sand/ for
-- more info.
--
-- NULLs do not take any space except for the null bitmask (basically free for less than 8 nullable columns). So use
-- NULL for default values when possible.
create table pgxjob_jobs (
	id bigint primary key generated by default as identity,
	inserted_at timestamptz not null,
	next_run_at timestamptz, -- if null then it is equal to inserted_at. This saves 8 bytes in the common case that the job should be run immediately.
	run_at timestamptz, -- if null then it is equal to next_run_at. This saves 8 bytes in the common case that the job doesn't need to be retried.
	group_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_groups rows are never deleted.
	type_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_types rows are never deleted.
	error_count int, -- if null then error_count = 0. This saves 4 bytes in the common case that the job hasn't errored.
	last_error text,
	params json -- use json instead of jsonb as it is faster for insert.
);

create table pgxjob_job_runs (
	job_id bigint not null, -- no foreign key because original jobs will be deleted
	inserted_at timestamptz not null,
	run_at timestamptz not null,

	-- not using tstzrange because jobs which take less than a microsecond may have the same value for started_at and
	-- finished_at because PostgreSQL only has microsecond level precision. A [) range where both ends have the same value
	-- is "empty". This special value would lose the time that the job actually ran.
	started_at timestamptz not null,
	finished_at timestamptz not null,

	run_number int not null,
	group_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_groups rows are never deleted.
	type_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_types rows are never deleted.
	params json,
	error text,
	primary key (job_id, run_number)
);
