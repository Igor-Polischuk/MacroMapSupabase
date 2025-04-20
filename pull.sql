create or replace function pull_changes(last_pulled_at bigint, pull_user_id uuid)
returns jsonb
language plpgsql
security definer
as $$
declare
  last_sync timestamp with time zone;
  current_sync timestamp with time zone := now();
  result jsonb;
begin
  if last_pulled_at is null then
    last_sync := to_timestamp(0);
  else
    last_sync := to_timestamp(last_pulled_at / 1000.0);
  end if;

  result := jsonb_build_object(
    'quick_entries', jsonb_build_object(
      'created', (
        select coalesce(jsonb_agg(jsonb_build_object(
          'id', id,
          'user_id', user_id,
          'kcal', kcal,
          'protein', protein,
          'fat', fat,
          'carbs', carbs,
          'created_at', timestamp_to_epoch(created_at),
          'updated_at', timestamp_to_epoch(updated_at)
        )), '[]'::jsonb)
        from quick_entries
        where created_at > last_sync and user_id = pull_user_id and deleted_at is null
      ),
      'updated', (
        select coalesce(jsonb_agg(jsonb_build_object(
          'id', id,
          'user_id', user_id,
          'kcal', kcal,
          'protein', protein,
          'fat', fat,
          'carbs', carbs,
          'created_at', timestamp_to_epoch(created_at),
          'updated_at', timestamp_to_epoch(updated_at)
        )), '[]'::jsonb)
        from quick_entries
        where updated_at > last_sync and created_at <= last_sync and user_id = pull_user_id and deleted_at is null
      ),
      'deleted',  (
  select coalesce(jsonb_agg(id::text), '[]'::jsonb)
  from quick_entries
  where deleted_at > last_sync and user_id = pull_user_id
)
    ),
    'logged_entries', jsonb_build_object(
      'created', (
        select coalesce(jsonb_agg(jsonb_build_object(
          'id', id,
          'user_id', user_id,
          'date', timestamp_to_epoch(date::timestamp with time zone),
          'name', name,
          'quick_entry_id', quick_entry_id,
          'created_at', timestamp_to_epoch(created_at),
          'updated_at', timestamp_to_epoch(updated_at)
        )), '[]'::jsonb)
        from logged_entries
        where created_at > last_sync and user_id = pull_user_id and deleted_at is null
      ),
      'updated', (
        select coalesce(jsonb_agg(jsonb_build_object(
          'id', id,
          'user_id', user_id,
          'date', timestamp_to_epoch(date::timestamp with time zone),
          'name', name,
          'quick_entry_id', quick_entry_id,
          'created_at', timestamp_to_epoch(created_at),
          'updated_at', timestamp_to_epoch(updated_at)
        )), '[]'::jsonb)
        from logged_entries
        where updated_at > last_sync and created_at <= last_sync and user_id = pull_user_id and deleted_at is null
      ),
      'deleted', (
  select coalesce(jsonb_agg(id::text), '[]'::jsonb)
  from logged_entries
  where deleted_at > last_sync and user_id = pull_user_id
)
    )
  );

  return jsonb_build_object(
    'changes', result,
    'timestamp', floor(extract(epoch from current_sync) * 1000)::bigint
  );
end;
$$;