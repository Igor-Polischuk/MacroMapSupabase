create or replace function pull_changes (last_pulled_at bigint, pull_user_id uuid) RETURNS jsonb LANGUAGE plpgsql as $$
DECLARE
  since         timestamptz := to_timestamp(coalesce(last_pulled_at, 0) / 1000.0);
  tbl           record;
  obj           jsonb := '{}'::jsonb;
  created_data  jsonb;
  updated_data  jsonb;
  deleted_data  jsonb;
BEGIN
  FOR tbl IN SELECT * FROM sync_tables LOOP
    BEGIN
      IF tbl.name = 'products' THEN
        -- ==== СПЕЦІАЛЬНО ДЛЯ PRODUCTS ====

        -- 1) Нові продукти
        EXECUTE '
          WITH used AS (
            SELECT product_id FROM logged_products     WHERE user_id = $2 and created_at > $1
            UNION
            SELECT product_id FROM logged_meal_items   WHERE user_id = $2  and created_at > $1
          )
          SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
          FROM products t
          WHERE t.deleted_at IS NULL
            AND (
              (t.created_at > $1 AND t.created_by = $2)
           OR (t.is_global     = TRUE
               AND t.id IN (SELECT product_id FROM used))
            )'
        INTO created_data
        USING since, pull_user_id;

        -- 2) Оновлені продукти
        EXECUTE '
          WITH used AS (
            SELECT product_id FROM logged_products     WHERE user_id = $2 and updated_at > $1
            UNION
            SELECT product_id FROM logged_meal_items   WHERE user_id = $2 and updated_at > $1
          )
          SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
          FROM products t
          WHERE t.deleted_at IS NULL
            AND t.updated_at  > $1
            AND t.created_at <= $1
            AND (
              t.created_by = $2
           OR (t.is_global = TRUE
               AND t.id IN (SELECT product_id FROM used))
            )'
        INTO updated_data
        USING since, pull_user_id;

        -- 3) Видалені продукти
        EXECUTE '
          WITH used AS (
            SELECT product_id FROM logged_products     WHERE user_id = $2 and deleted_at > $1
            UNION
            SELECT product_id FROM logged_meal_items   WHERE user_id = $2 and deleted_at > $1
          )
          SELECT coalesce(jsonb_agg(t.id::text), ''[]'')
          FROM products t
          WHERE t.deleted_at > $1
            AND (
              t.created_by = $2
           OR (t.is_global = TRUE
               AND t.id IN (SELECT product_id FROM used))
            )'
        INTO deleted_data
        USING since, pull_user_id;

      ELSIF tbl.join_table IS NULL THEN
        -- ======== Інші “прості” таблиці з user_column ========
        EXECUTE format(
          'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             FROM %I t
            WHERE t.created_at > $1
              AND t.%I = $2
              AND t.deleted_at IS NULL',
          tbl.name, tbl.user_column
        )
        INTO created_data
        USING since, pull_user_id;

        EXECUTE format(
          'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             FROM %I t
            WHERE t.updated_at > $1
              AND t.created_at <= $1
              AND t.%I = $2
              AND t.deleted_at IS NULL',
          tbl.name, tbl.user_column
        )
        INTO updated_data
        USING since, pull_user_id;

        EXECUTE format(
          'SELECT coalesce(jsonb_agg(t.id::text), ''[]'')
             FROM %I t
            WHERE t.deleted_at > $1
              AND t.%I = $2',
          tbl.name, tbl.user_column
        )
        INTO deleted_data
        USING since, pull_user_id;

      ELSE
        -- ======== Таблиці через JOIN ========
        EXECUTE format(
          'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             FROM %I t
             JOIN %I j ON t.%I = j.id
            WHERE t.created_at > $1
              AND j.%I = $2
              AND t.deleted_at IS NULL',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column
        )
        INTO created_data
        USING since, pull_user_id;

        EXECUTE format(
          'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]'')
             FROM %I t
             JOIN %I j ON t.%I = j.id
            WHERE t.updated_at > $1
              AND t.created_at <= $1
              AND j.%I = $2
              AND t.deleted_at IS NULL',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column
        )
        INTO updated_data
        USING since, pull_user_id;

        EXECUTE format(
          'SELECT coalesce(jsonb_agg(t.id::text), ''[]'')
             FROM %I t
             JOIN %I j ON t.%I = j.id
            WHERE t.deleted_at > $1
              AND j.%I = $2',
          tbl.name, tbl.join_table, tbl.join_column, tbl.join_user_column
        )
        INTO deleted_data
        USING since, pull_user_id;
      END IF;

      obj := obj || jsonb_build_object(
        tbl.name,
        jsonb_build_object(
          'created', created_data,
          'updated', updated_data,
          'deleted', deleted_data
        )
      );

    EXCEPTION
      WHEN others THEN
        RAISE EXCEPTION
          'Error processing table "%": column "%". Message: %',
          tbl.name,
          coalesce(tbl.user_column, tbl.join_user_column),
          SQLERRM;
    END;
  END LOOP;

  RETURN jsonb_build_object(
    'changes',   obj,
    'timestamp', (extract(epoch FROM clock_timestamp()) * 1000)::bigint
  );
END;
$$;