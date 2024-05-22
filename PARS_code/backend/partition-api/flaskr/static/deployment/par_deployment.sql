-- %%%%%%%%%%%%%Vertical Partition Scheme
-- 1. Partitioned Table: %s
-- 2. Partitions: %s
-- 3. Algorithm: %s
-- 4. SQL Call Statement: SELECT vertical_partition_fun(@1,@2);
-- %%%%%%%%%%%%%Column Group creation Statements
CREATE OR REPLACE FUNCTION "VP"."vertical_partition_fun"("tablename" varchar, "columngroup" text)
  RETURNS "pg_catalog"."text" AS $BODY$
    DECLARE 
        partitions text[][];
        par text[];
        field record;
        viewKey text;
        view Columns text;
        viewSubColumns text;
        viewJoin text;
        create_sql text DEFAULT '';
        insert_sql text DEFAULT '';
        create_view_sql text DEFAULT '';
        i int :=1;
    BEGIN
        -- Routine body goes here...
        partitions:=columngroup::text[][];
--      partitions:=ARRAY[['sname','smajor'],['ssex','sage']];
--      Delete view if it exists
            EXECUTE 'drop view if EXISTS '||tablename||'_view'||';';
FOR i IN array_lower(partitions,1)..array_upper(partitions,1) LOOP
--  Delete subtables if they exist
    EXECUTE 'drop table if EXISTS '||tablename||'_'||i||' CASCADE';
    FOR field IN EXECUTE format($$select column_name,data_type,character_maximum_length from (select * from information_schema.columns where table_name='%1$s') s limit 1;$$,tablename) LOOP
       create_sql:=format($$create table %1$s_%2$s (%3$s %4$s primary key,$$,tablename,i,field.column_name,field.data_type);
       viewKey:=field.column_name;
       insert_sql:=format($$insert into %1$s_%2$s select %3$s,$$,tablename,i,field.column_name);
       END LOOP;
            
       if i=1 THEN
          view Columns:=viewKey||',';
          viewJoin:=tablename||'_'||i||' ';
       ELSE
          viewJoin:=viewJoin||format($$INNER JOIN %1$s_%2$s USING(%3$s)$$,tablename,i,viewKey);
       END IF;
FOR j IN array_lower(partitions,2)..array_upper(partitions,2) LOOP
    view Columns:=view Columns||partitions[i][j]||',';
                
    FOR field IN EXECUTE format($$select column_name,data_type,character_maximum_length from (select * from information_schema.columns where table_name='%1$s') s where s.column_name='%2$s';$$,tablename,partitions[i][j]) LOOP
        IF field.character_maximum_length is NULL THEN
           create_sql:=create_sql||format($$%1$s %2$s,$$,field.column_name,field.data_type);
        ELSE
           create_sql:=create_sql||format($$%1$s %2$s(%3$s),$$,field.column_name,field.data_type,field.character_maximum_length);
        END IF;
        insert_sql:=insert_sql||field.column_name||',';
    END LOOP;
END LOOP;
            create_sql:=substr(create_sql,0,length(create_sql));
            create_sql:=create_sql||');';
            insert_sql:=substr(insert_sql,0,length(insert_sql));
            insert_sql:=insert_sql||' from '||tablename||';';
            RAISE NOTICE '%',create_sql;
            RAISE NOTICE '%',insert_sql;
            EXECUTE create_sql;
            EXECUTE insert_sql;
        END LOOP;
-- Column Group Views 
        view Columns:=substr(viewColumns,0,length(viewColumns));
        create_view_sql:=format($$CREATE VIEW %1$s_view(%2$s) AS SELECT %3$s FROM %4$s;$$,tablename,viewColumns,viewColumns,viewJoin)tablename;
        RAISE NOTICE '%',create_view_sql;
        EXECUTE create_view_sql;
        
        RETURN 'Vertical Partitioning has been finished!';
    END$BODY$
LANGUAGE plpgsql VOLATILE