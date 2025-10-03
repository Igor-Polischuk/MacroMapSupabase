-- drop function if exists public.search_products(
--   text, text, text, text, boolean, integer, integer
-- );
create or replace function public.search_products(
  _q            text    default null,
  _lang         text    default null,
  _category     text    default null,
  _brand        text    default null,
  _is_verified  boolean default null,
  _limit        int     default 25,
  _offset       int     default 0
)
returns table (
  id               uuid,
  name             text,
  brand            text,
  category         text,
  protein          numeric,
  fat              numeric,
  carbs            numeric,
  calories         numeric,
  "isVerified"     boolean,
  "isGlobal"       boolean,
  provider         text,
  "imageUrl" text,
  "translationName"  text
)
language sql
stable
as $$
  select
    p.id,
    p.name,
    p.brand,
    p.category,
    p.protein,
    p.fat,
    p.carbs,
    p.calories,
    p.is_verified       as "isVerified",
    p.is_global         as "isGlobal",
    p.provider,
    p.image_url as "imageUrl",
    -- повертаємо переклад ДЛЯ вказаної локалі; якщо _lang не задано або перекладу нема — null
    (
      select t.name
      from public.product_translations t
      where t.product_id = p.id
        and (_lang is not null and t.locale = _lang)
      limit 1
    ) as "translationName"
  from public.products p
  where p.is_global = true
    -- пошук:
    and (
      _q is null
      or p.name ilike '%' || _q || '%'
      or (
        _lang is not null
        and exists (
          select 1
          from public.product_translations t2
          where t2.product_id = p.id
            and t2.locale = _lang
            and t2.name ilike '%' || _q || '%'
        )
      )
    )
    -- додаткові фільтри (опціональні)
    and (_category    is null or p.category    = _category)
    and (_brand       is null or p.brand       ilike '%' || _brand || '%')
    and (_is_verified is null or p.is_verified = _is_verified)
  order by p.name
  limit _limit offset _offset;
$$;
