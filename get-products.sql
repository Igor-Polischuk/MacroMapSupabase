create or replace function public.search_products(
  _q text default null,
  _lang text default null,
  _category text default null,
  _brand text default null,
  _is_verified boolean default null,
  _limit int default 50,
  _offset int default 0
)
returns table (
  id uuid,
  name text,
  brand text,
  category text,
  protein numeric,
  fat numeric,
  carbs numeric,
  calories numeric,
  is_verified boolean,
  is_global boolean,
  provider text,
  image_storage_path text,
  translation_name text
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
    p.is_verified,
    p.is_global,
    p.provider,
    p.image_storage_path,
    t.name as translation_name
  from public.products p
  left join public.product_translations t
    on t.product_id = p.id
   and (_lang is null or t.locale = _lang)
  where p.is_global = true
    and (
      _q is null
      or p.name ilike '%' || _q || '%'
      or (
        _lang is not null and t.name ilike '%' || _q || '%'
      )
    )
    and (_category is null or p.category = _category)
    and (_brand is null or p.brand = _brand)
    and (_is_verified is null or p.is_verified = _is_verified)
  order by p.name
  limit _limit offset _offset;
$$;
