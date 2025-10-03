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

create or replace function push_changes(changes jsonb) returns void
  security definer
as $$
declare
  new_record      jsonb;
  updated_record  jsonb;
  deleted_id      uuid;
begin
  ----------------------------------------
  -- 1. Обробка quick_entries
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'quick_entries'->'created')
  loop
    insert into quick_entries (
      id, user_id, kcal, protein, fat, carbs, created_at, updated_at
    ) values (
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
      set kcal       = (updated_record->>'kcal')::real,
          protein    = (updated_record->>'protein')::real,
          fat        = (updated_record->>'fat')::real,
          carbs      = (updated_record->>'carbs')::real,
          updated_at = epoch_to_timestamp(updated_record->>'updated_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення (soft delete)
  for deleted_id in
    select jsonb_array_elements_text(changes->'quick_entries'->'deleted')
  loop
    update quick_entries
      set deleted_at = now()
    where id = deleted_id::uuid;
  end loop;

  ----------------------------------------
  -- 2. Обробка logged_entries
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'logged_entries'->'created')
  loop
    insert into logged_entries (
      id, user_id, date, name, quick_entry_id, created_at, updated_at
    ) values (
      (new_record->>'id')::uuid,
      (new_record->>'user_id')::uuid,
      to_date(new_record->>'date', 'YYYY-MM-DD'),
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
      set date           = to_date(updated_record->>'date', 'YYYY-MM-DD'),
          name           = updated_record->>'name',
          quick_entry_id = (updated_record->>'quick_entry_id')::uuid,
          updated_at     = epoch_to_timestamp(updated_record->>'updated_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення (soft delete)
  for deleted_id in
    select jsonb_array_elements_text(changes->'logged_entries'->'deleted')
  loop
    update logged_entries
      set deleted_at = now()
    where id = deleted_id::uuid;
  end loop;

  ----------------------------------------
  -- 3. Обробка products
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'products'->'created')
  loop
    insert into products (
      id, name, brand, barcode,
      is_global, is_verified, category,
      calories, protein, fat, carbs,
      created_by, created_at, updated_at
    ) values (
      (new_record->>'id')::uuid,
      new_record->>'name',
      new_record->>'brand',
      new_record->>'barcode',
      (new_record->>'is_global')::boolean,
      (new_record->>'is_verified')::boolean,
      new_record->>'category',
      (new_record->>'calories')::real,
      (new_record->>'protein')::real,
      (new_record->>'fat')::real,
      (new_record->>'carbs')::real,
      (new_record->>'created_by')::uuid,
      epoch_to_timestamp(new_record->>'created_at'),
      epoch_to_timestamp(new_record->>'updated_at')
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'products'->'updated')
  loop
    update products
      set name        = updated_record->>'name',
          brand       = updated_record->>'brand',
          barcode     = updated_record->>'barcode',
          is_global   = (updated_record->>'is_global')::boolean,
          is_verified = (updated_record->>'is_verified')::boolean,
          category    = updated_record->>'category',
          calories    = (updated_record->>'calories')::real,
          protein     = (updated_record->>'protein')::real,
          fat         = (updated_record->>'fat')::real,
          carbs       = (updated_record->>'carbs')::real,
          created_by  = (updated_record->>'created_by')::uuid,
          updated_at  = epoch_to_timestamp(updated_record->>'updated_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'products'->'deleted')
  loop
    delete from products where id = deleted_id::uuid;
  end loop;

  ----------------------------------------
  -- 4. Обробка product_serving_sizes
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'product_serving_sizes'->'created')
  loop
    insert into product_serving_sizes (
      id, product_id, name, size
    ) values (
      (new_record->>'id')::uuid,
      (new_record->>'product_id')::uuid,
      new_record->>'name',
      (new_record->>'size')::real
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'product_serving_sizes'->'updated')
  loop
    update product_serving_sizes
      set product_id = (updated_record->>'product_id')::uuid,
          name       = updated_record->>'name',
          size       = (updated_record->>'size')::real
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'product_serving_sizes'->'deleted')
  loop
    delete from product_serving_sizes where id = deleted_id::uuid;
  end loop;

  ----------------------------------------
  -- 5. Обробка product_nutrients
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'product_nutrients'->'created')
  loop
    insert into product_nutrients (
      id, product_id, nutrient_name, nutrient_type, value, unit
    ) values (
      (new_record->>'id')::uuid,
      (new_record->>'product_id')::uuid,
      new_record->>'nutrient_name',
      (new_record->>'nutrient_type')::nutrient_type_enum,
      (new_record->>'value')::real,
      new_record->>'unit'
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'product_nutrients'->'updated')
  loop
    update product_nutrients
      set product_id    = (updated_record->>'product_id')::uuid,
          nutrient_name = updated_record->>'nutrient_name',
          nutrient_type = (updated_record->>'nutrient_type')::nutrient_type_enum,
          value         = (updated_record->>'value')::real,
          unit          = updated_record->>'unit'
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'product_nutrients'->'deleted')
  loop
    delete from product_nutrients where id = deleted_id::uuid;
  end loop;

  ----------------------------------------
  -- 6. Обробка user_saved_products
  ----------------------------------------
  -- Створення
  for new_record in
    select jsonb_array_elements(changes->'user_saved_products'->'created')
  loop
    insert into user_saved_products (
      id, user_id, product_id, saved_at
    ) values (
      (new_record->>'id')::uuid,
      (new_record->>'user_id')::uuid,
      (new_record->>'product_id')::uuid,
      epoch_to_timestamp(new_record->>'saved_at')
    )
    on conflict (id) do nothing;
  end loop;

  -- Оновлення
  for updated_record in
    select jsonb_array_elements(changes->'user_saved_products'->'updated')
  loop
    update user_saved_products
      set saved_at = epoch_to_timestamp(updated_record->>'saved_at')
    where id = (updated_record->>'id')::uuid;
  end loop;

  -- Видалення
  for deleted_id in
    select jsonb_array_elements_text(changes->'user_saved_products'->'deleted')
  loop
    delete from user_saved_products where id = deleted_id::uuid;
  end loop;
end;
$$ language plpgsql;