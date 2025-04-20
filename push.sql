create or replace function epoch_to_timestamp (epoch text) returns timestamp with time zone as $$ begin return timestamp with time zone 'epoch' + ((epoch::bigint) / 1000) * interval '1 second';
end;
$$ language plpgsql;

create or replace function timestamp_to_epoch (ts timestamp with time zone) returns bigint as $$ begin return (
        extract(
            epoch
            from ts
        ) * 1000
    )::bigint;
end;
$$ language plpgsql;

create or replace function push_changes (changes jsonb) returns void SECURITY DEFINER as $$
declare
  new_record jsonb;
  updated_record jsonb;
  deleted_id uuid;
begin
  -- Обробка quick_entries
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'quick_entries'->'created')
  loop
    insert into quick_entries (
      id,
      user_id,
      kcal,
      protein,
      fat,
      carbs,
      created_at,
      updated_at
    )
    values (
      (new_record->>'id')::uuid,
      (new_record->>'user_id')::uuid,
      (new_record->>'kcal')::real,
      (new_record->>'protein')::real,
      (new_record->>'fat')::real,
      (new_record->>'carbs')::real,
      epoch_to_timestamp(new_record->>'created_at'),
      epoch_to_timestamp(new_record->>'updated_at')
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'quick_entries'->'updated')
  loop
    update quick_entries
    set
      kcal = (updated_record->>'kcal')::real,
      protein = (updated_record->>'protein')::real,
      fat = (updated_record->>'fat')::real,
      carbs = (updated_record->>'carbs')::real,
      updated_at = epoch_to_timestamp(new_record->>'updated_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Обробка logged_entries
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'logged_entries'->'created')
  loop
    insert into logged_entries (
      id,
      user_id,
      date,
      name,
      quick_entry_id,
      created_at,
      updated_at
    )
    values (
      (new_record->>'id')::uuid,
      (new_record->>'user_id')::uuid,
      epoch_to_timestamp(new_record->>'date')::date,
      new_record->>'name',
      (new_record->>'quick_entry_id')::uuid,
      epoch_to_timestamp(new_record->>'created_at'),
      epoch_to_timestamp(new_record->>'updated_at')
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'logged_entries'->'updated')
  loop
    update logged_entries
    set
      date = to_date(updated_record->>'date', 'YYYY-MM-DD'),
      name = updated_record->>'name',
      quick_entry_id = (updated_record->>'quick_entry_id')::uuid,
      updated_at = epoch_to_timestamp(new_record->>'updated_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'logged_entries'->'deleted')
  loop
    delete from logged_entries where id = deleted_id::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'quick_entries'->'deleted')
  loop
    delete from quick_entries where id = deleted_id::uuid;
  end loop;
end;
$$ language plpgsql;