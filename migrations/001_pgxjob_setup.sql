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
	heartbeat timestamptz not null,
	group_id int not null references pgxjob_groups
);

create sequence pgxjob_workers_id_seq as int cycle owned by pgxjob_workers.id;
alter table pgxjob_workers alter column id set default nextval('pgxjob_workers_id_seq');

-- pgxjob_asap_jobs and pgxjob_run_at_jobs can be a very hot tables. We want to keep them as small as possible.
--
-- Columns are carefully ordered to avoid wasted space. See https://www.2ndquadrant.com/en/blog/on-rocks-and-sand/ for
-- more info.
--
-- Both tables share the same sequence.

create sequence pgxjob_jobs_id_seq as bigint;

-- pgxjob_asap_jobs is a queue of jobs that should be run as soon as possible.
create table pgxjob_asap_jobs (
	id bigint primary key default nextval('pgxjob_jobs_id_seq'),
	inserted_at timestamptz not null default now(),
	group_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_groups rows are never deleted.
	type_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_types rows are never deleted.
	worker_id int references pgxjob_workers on delete set null,
	params json -- use json instead of jsonb as it is faster for insert.
);

-- pgxjob_run_at_jobs is a queue of jobs that should be run at a specific time.
create table pgxjob_run_at_jobs (
	id bigint primary key default nextval('pgxjob_jobs_id_seq'),
	inserted_at timestamptz not null default now(),
	run_at timestamptz not null,
	next_run_at timestamptz not null,
	group_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_groups rows are never deleted.
	type_id int not null, -- purposely not a foreign key for best insert performance. pgxjob_types rows are never deleted.
	worker_id int references pgxjob_workers on delete set null,
	error_count int not null,
	last_error text,
	params json -- use json instead of jsonb as it is faster for insert.
);

create index pgxjob_run_at_jobs_next_run_at_idx on pgxjob_run_at_jobs (next_run_at);

create table pgxjob_job_runs (
	job_id bigint not null, -- no foreign key because original jobs will be deleted
	job_inserted_at timestamptz not null,
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

create index pgxjob_job_runs_finished_at_idx on pgxjob_job_runs using brin (job_id);
