-- Tabela para vendas brutas importadas de scrapers
create table if not exists raw_sales (
  id uuid primary key default gen_random_uuid(),
  domain_name text not null,
  price numeric not null check (price > 0),
  currency text default 'USD',
  date date not null,
  platform text,
  source_url text,
  created_at timestamp default now()
);

-- Tabela para registo de erros de ETL
create table if not exists etl_log (
  id uuid primary key default gen_random_uuid(),
  task_name text,
  status text,
  message text,
  timestamp timestamp default now()
);
