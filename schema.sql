create table if not exists top_casts (
  cast_hash text primary key,
  fid bigint,
  text text,
  channel text,
  timestamp timestamptz not null,
  likes int default 0,
  recasts int default 0,
  replies int default 0,
  score int generated always as (
    coalesce(replies,0)*10 + coalesce(recasts,0)*3 + coalesce(likes,0)
  ) stored
);

create index if not exists idx_top_casts_ts on top_casts (timestamp desc);
create index if not exists idx_top_casts_channel_ts on top_casts (channel, timestamp desc);

