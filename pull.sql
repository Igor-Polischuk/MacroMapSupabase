
declare
  since         timestamptz := to_timestamp(coalesce(last_pulled_at, 0) / 1000.0);
  tbl           record;
  obj           jsonb := '{}'::jsonb;
  created_data  jsonb;
  updated_data  jsonb;
  deleted_data  jsonb;
begin
  for tbl in select * from sync_tables loop
    begin
      if tbl.join_table is null then
        -- ======== Звичайна таблиця з user_column ========
        execute format(
          'select coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             from %I t
            where t.created_at > $1 and t.%I = $2 and t.deleted_at is null',
          tbl.name, tbl.user_column)
        into created_data
        using since, pull_user_id;

        execute format(
          'select coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             from %I t
            where t.updated_at > $1 and t.created_at <= $1 and t.%I = $2 and t.deleted_at is null',
          tbl.name, tbl.user_column)
        into updated_data
        using since, pull_user_id;

        execute format(
          'select coalesce(jsonb_agg(t.id::text), ''[]'')
             from %I t
            where t.deleted_at > $1 and t.%I = $2',
          tbl.name, tbl.user_column)
        into deleted_data
        using since, pull_user_id;

      else
        -- ======== Таблиця через JOIN ========
        execute format(
          'select coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             from %I t
             join %I j on t.%I = j.id
            where t.created_at > $1 and j.%I = $2 and t.deleted_at is null',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column)
        into created_data
        using since, pull_user_id;

        execute format(
          'select coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             from %I t
             join %I j on t.%I = j.id
            where t.updated_at > $1 and t.created_at <= $1 and j.%I = $2 and t.deleted_at is null',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column)
        into updated_data
        using since, pull_user_id;

        execute format(
          'select coalesce(jsonb_agg(t.id::text), ''[]'')
             from %I t
             join %I j on t.%I = j.id
            where t.deleted_at > $1 and j.%I = $2',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column)
        into deleted_data
        using since, pull_user_id;
      end if;

      obj := obj || jsonb_build_object(
        tbl.name,
        jsonb_build_object(
          'created', created_data,
          'updated', updated_data,
          'deleted', deleted_data
        )
      );

    exception when others then
      raise exception
        'Error during processing "%": column "%". Message: %',
        tbl.name,
        coalesce(tbl.user_column, tbl.join_user_column),
        SQLERRM;
    end;
  end loop;

  return jsonb_build_object(
    'changes',   obj,
    'timestamp', (extract(epoch from clock_timestamp()) * 1000)::bigint
  );
end;
