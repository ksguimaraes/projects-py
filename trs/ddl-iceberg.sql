-- DDL para pré-criação da tabela Iceberg antes de rodar o job.
-- O job lê o schema desta tabela em runtime para montar o reader do CSV sem cabeçalho.
-- A LOCATION deve ser diferente do path ORC da tabela Hive legada para evitar conflitos.

CREATE TABLE IF NOT EXISTS glue_catalog.trs.admins (
  admin_id    INT,
  username    STRING,
  passwd      STRING,
  fullname    STRING,
  jabbername  STRING,
  email       STRING,
  mobile      STRING,
  status      STRING
)
USING iceberg
LOCATION 's3://olxbr-dl-raw-standard/olx/iceberg/trs/admins'
TBLPROPERTIES (
  'format-version'          = '2',
  'write.format.default'    = 'parquet',
  'olx.team'                = 'Data Engineering'
);
