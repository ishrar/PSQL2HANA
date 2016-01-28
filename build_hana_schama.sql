-- Copyright (c) 2016 Ishrar Hussain

CREATE OR REPLACE FUNCTION build_hana_schema(source_schema TEXT, path TEXT) RETURNS void AS $$
declare
  tables RECORD;
  cols RECORD;
  statement TEXT;
begin
  statement := 'CREATE SCHEMA IF NOT EXISTS hana_' || source_schema;
  EXECUTE statement;
  FOR tables IN 
    SELECT table_name AS schema_table
    FROM information_schema.tables t INNER JOIN information_schema.schemata s 
    ON s.schema_name = t.table_schema 
    WHERE t.table_schema = source_schema
    ORDER BY schema_table
  LOOP
    statement := 'CREATE TABLE hana_' || source_schema || '.' || tables.schema_table || ' AS (SELECT * FROM ' || source_schema || '.' || tables.schema_table || ')';
    EXECUTE statement;
    FOR cols IN 
      select column_name AS table_column
      from information_schema.columns where data_type like 'char%' and character_octet_length > 12
      and table_schema = 'hana_' || source_schema and table_name = tables.schema_table
    LOOP
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REPLACE(' || cols.table_column || ', ''\'',''&#92'')';
      EXECUTE statement;
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REGEXP_REPLACE(' || cols.table_column || ', E''[\\n\\r]+'',''<br />'', ''g'')';
      EXECUTE statement;
    END LOOP;
    FOR cols IN 
      select column_name AS table_column
      from information_schema.columns where data_type like 'text%' and character_octet_length > 12
      and table_schema = 'hana_' || source_schema and table_name = tables.schema_table
    LOOP
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REPLACE(' || cols.table_column || ', ''\'',''&#92'')';
      EXECUTE statement;
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REGEXP_REPLACE(' || cols.table_column || ', E''(.+)'',''<p>\1</p>'')';
      EXECUTE statement;
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REGEXP_REPLACE(' || cols.table_column || ', E''(\\n\\r\\n\\r)+'',''</p><p>'', ''g'')';
      EXECUTE statement;
      statement := 'UPDATE hana_' || source_schema || '.' || tables.schema_table || ' SET ' || cols.table_column || ' = REGEXP_REPLACE(' || cols.table_column || ', E''[\\n\\r]+'',''<br />'', ''g'')';
      EXECUTE statement;
    END LOOP;
    statement := 'COPY hana_' || source_schema || '.' || tables.schema_table || ' TO ''' || path || '/' || tables.schema_table || '.csv'' WITH CSV HEADER QUOTE AS ''"'' ESCAPE AS ''\''';
    EXECUTE statement;
    
  END LOOP;
  return;  
end;
$$ LANGUAGE plpgsql;