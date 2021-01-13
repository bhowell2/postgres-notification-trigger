/*
  Table trg_notifs.

  Table containing information on trigger notifications. This is more
  convenient to use than fn_create_notification_trigger(), because it
  allows for easier viewing of the existing trigger notifications
  (created through it) in the database. This allows the user to insert,
  update, or delete from the table and the required notification
  function/trigger will be created. This is not required and the user
  may call fn_create_notification_trigger (which is called by a trigger
  on this table); however the created function and trigger will not
  be tracked by this table.

  When inserting/updating/deleting from this table, a trigger is called
  that acquires a lock on the hashed name of '{table_name}_trg_notif_lock'.
  This ensures that concurrent operations for the same table do not cross
  (one could write one function and the other the trigger, where the columns
  would be different as the columns are not included in the trigger name).

  There is one edge case here that is not handled as it probably won't happen
  often - when the generated function name is longer than 63 (by default) bytes
  it will be truncated and therefore some information could be lost. This could
  be easily overcome by changing the generated trg_fn_name and trg_name to something
  hashed (e.g., md5). The reason this was not done right now as it would make the
  generated trigger and function imperceptible if they were created from
  fn_create_notification_trigger() instead of inserting into this table - the risk
  of a truncation seems low as the event names have already been truncated to 'i'
  (insert), d (delete) and u (update) for the function/trigger names.

*/
CREATE TABLE IF NOT EXISTS trg_notifs (
  /*
    Unique ID for easy reference.
  */
  id serial PRIMARY KEY,
  /*
    Table to watch for the specified changes.
  */
  table_name text NOT NULL,
  /*
    The channel where the notification will be sent.
  */
  channel_name text NOT NULL,
  /*
    Optional. Will be supplied with the notification under the 'name' field
    to the specified channel.
  */
  notif_name text,  -- optionally supply a name to be returned with the notification
  /*
    There are multiple column options:
    1. __all__                              - will send back every column in the row
    2. __changed__ [, col1, col2, ...]      - will send back only the changed columns and any additional columns provided
                                              (on insert and delete this will send back all columns)
    3. col1, col2, ... colN                 - will send back every column listed
  */
  columns text[] NOT NULL,
  /*
    INSERT, UPDATE, or DELETE.
    Note, these can be inserted in any order and will be sorted.
  */
  events text[] NOT NULL,
  /*
    Any value supplied here will be overwritten by the trigger, which
    ensures that the trigger/function name are generated in a reproducible
    manner. This helps reduce collisions and truncations of the names.
  */
  trg_fn_name text NOT NULL,
  /*
    Any value supplied here will be overwritten by the trigger, which
    ensures that the trigger/function name are generated in a reproducible
    manner.
  */
  trg_name text NOT NULL
);

CREATE OR REPLACE FUNCTION fn_sort_array(ary text[]) RETURNS text[] AS $$
SELECT ARRAY(SELECT unnest(ary) order by 1)
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION fn_immutable_concat_str(s text[]) RETURNS TEXT AS $$
SELECT CONCAT(VARIADIC fn_sort_array(s))
$$ LANGUAGE sql IMMUTABLE ;

/*
  To avoid duplicate notifications, the uniqueness is created based on
  the table, channel, notification name, and events subscribed.
*/
CREATE UNIQUE INDEX idx_trg_notifs_uniqueness
  ON trg_notifs (
                 table_name,
                 channel_name,
                 notif_name,
                 fn_immutable_concat_str(events)
    );

/*
  This will be run before the trigger notifications table is modified,
  calling fn_create_notification_trigger with the appropriate value and
  then setting the trg_fn_name and trg_name columns in the table so that
  the function and trigger can be tracked with the table.
*/
CREATE OR REPLACE FUNCTION trg_fn_before_trg_notifs() RETURNS TRIGGER AS $$
DECLARE
  create_trg_return_rec record;
BEGIN

  IF tg_op = 'INSERT' THEN
    PERFORM PG_ADVISORY_XACT_LOCK(hashtext(new.table_name || '_trg_notif_lock'));
    SELECT * INTO new.columns FROM (SELECT fn_sort_array(new.columns)) dnc;
    SELECT * INTO new.events FROM (SELECT fn_sort_array(new.events)) dnc;
    SELECT * INTO create_trg_return_rec FROM
      fn_create_notification_trigger(new.table_name, new.channel_name, new.columns, new.events, new.notif_name);
    new.trg_name := create_trg_return_rec.trg_name;
    new.trg_fn_name := create_trg_return_rec.trg_fn_name;

    RETURN new;
  ELSEIF tg_op = 'UPDATE' THEN
    /*
      When an update occurs the old trigger should be removed and a new one
      created. This is because the created trigger function has some "hard-coded"
      values for the table name, column names, events, and channel.

      A lock is acquired on the table name (with notif) to avoid conflicting
      updates on the table. Locks old and new in case of change.
    */

    PERFORM PG_ADVISORY_XACT_LOCK(hashtext(new.table_name || '_trg_notif_lock'));
    PERFORM PG_ADVISORY_XACT_LOCK(hashtext(old.table_name || '_trg_notif_lock'));
    RAISE NOTICE 'Dropping trigger %s on %s', old.trg_name, old.table_name;
    RAISE NOTICE 'Dropping function %s', old.trg_fn_name;
    EXECUTE FORMAT('DROP TRIGGER IF EXISTS %s ON %s', old.trg_name, old.table_name);
    EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s()', old.trg_fn_name);

    SELECT * INTO new.columns FROM (SELECT fn_sort_array(new.columns)) dnc;
    SELECT * INTO new.events FROM (SELECT fn_sort_array(new.events)) dnc;
    SELECT * INTO create_trg_return_rec FROM
      fn_create_notification_trigger(new.table_name, new.channel_name, new.columns, new.events, new.notif_name);
    new.trg_name := create_trg_return_rec.trg_name;
    new.trg_fn_name := create_trg_return_rec.trg_fn_name;

    RETURN new;
  ELSEIF tg_op = 'DELETE' THEN

    PERFORM PG_ADVISORY_XACT_LOCK(hashtext(old.table_name || '_trg_notif_lock'));
    RAISE NOTICE 'Dropping trigger %s on %s', old.trg_name, old.table_name;
    RAISE NOTICE 'Dropping function %s', old.trg_fn_name;
    EXECUTE FORMAT('DROP TRIGGER IF EXISTS %s ON %s', old.trg_name, old.table_name);
    EXECUTE FORMAT('DROP FUNCTION IF EXISTS %s()', old.trg_fn_name);

    RETURN old;
  ELSE
    RAISE EXCEPTION 'Should have never been called for anything other than insert, update, or delete.';
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_before_trg_notifs
  BEFORE INSERT OR UPDATE OR DELETE
  ON trg_notifs
  FOR EACH ROW EXECUTE PROCEDURE trg_fn_before_trg_notifs();

/*
  This does not use the table above and is called by a trigger on the trg_notifs
  table. If the user does not need the table they may simply use this function
  instead and not create it.
*/
CREATE OR REPLACE FUNCTION fn_create_notification_trigger(
  tableName text,
  channelName text,
  columns text[], -- either '{__all__}', '{__changes__}', or '{col1, col2, ... colN}' (can be combined with __changes__).
  events text[], -- an array of INSERT, UPDATE, and/or DELETE
  notifName text, -- if null will result in null value for 'name' field in the generated notification
  OUT trg_fn_name text,
  OUT trg_name text
) as $a$
DECLARE
  trg_event text;
  /*
    If __all__ is not supplied and either __changes__ with additional columns or column names are
    supplied this will be populated with the columns that will be gathered for the notification.
  */
  ret_columns text[];
  array_text text; -- to loop over columns
  execute_create_fn_statement text := '';
BEGIN

  IF tableName IS NULL THEN
    RAISE EXCEPTION 'Cannot call with NULL table name.';
  END IF;

  IF channelName IS NULL THEN
    RAISE EXCEPTION 'Cannot call with NULL channel name.';
  END IF;

  IF array_length(columns,1) IS NULL OR array_length(columns,1) <= 0 THEN
    RAISE EXCEPTION 'Columns must be provided for notifications. This may be "__all__", "__changes__" or the name of each column to return.';
  END IF;

  -- i think this will always return null when array is empty, but checking <= 0 for good measure too.
  IF array_length(events, 1) IS NULL OR array_length(events, 1) <= 0 THEN
    RAISE EXCEPTION 'At least one trigger event (events array parameter) must be provided for notifications. Was an empty array. This may be "insert", "update", and/or "delete".';
  END IF;

  FOREACH array_text IN ARRAY events LOOP
    IF LOWER(array_text) <> 'insert' AND lower(array_text) <> 'update' AND lower(array_text) <> 'delete' THEN
      RAISE EXCEPTION 'Invalid trigger events supplied. Supplied array = %.', events;
    END IF;
  END LOOP;

  /*
    Generate the trigger/trigger-function name. This will include the table name,
    channel name, and the events (allowing multiple notification triggers to
    be created for the different events on the same channel).
  */
  trg_name := FORMAT(
      'trg_notify_%s_for_%s_events_%s%s',
      channelName,
      tableName,
      -- will be d_i_u for delete, insert, update. sorted so independent of order provided
      CONCAT_WS('_', VARIADIC (SELECT ARRAY(SELECT SUBSTRING(UNNEST(events), 1, 1) ORDER BY 1))),
      -- the notif name will be appended if it exists (need to add underscore with it)
      CASE WHEN notifName IS NOT NULL THEN '_' || notifName ELSE '' END
    );

  trg_fn_name := FORMAT(
      'trg_fn_notify_%s_for_%s_events_%s%s',
      channelName,
      tableName,
    -- will be d_i_u for delete, insert, update. sorted so independent of order provided
      CONCAT_WS('_', VARIADIC (SELECT ARRAY(SELECT SUBSTRING(UNNEST(events), 1, 1) ORDER BY 1))),
      -- the notif name will be appended if it exists (need to add underscore with it)
      CASE WHEN notifName IS NOT NULL THEN '_' || notifName ELSE '' END
    );

  /*
    Create the trigger function to be called when an event of the provided
    event types occurs.
  */

  execute_create_fn_statement :=
      'CREATE OR REPLACE FUNCTION ' || quote_ident(trg_fn_name) || '() RETURNS TRIGGER AS $t$ '
          || ' DECLARE'
          || '  old_jsonb jsonb;'
          || '  new_jsonb jsonb;'
          || '  each_rec record;'
          || '  notif_name text := ' || quote_nullable(notifName) || ';'
        -- need to start off with empty JSON to concat with it in loop if necessary
          || '  notif_data jsonb := ''{}'' ;'
          || '  notif jsonb;'
          || ' BEGIN ';

  /*
    Requiring __all__ to be first argument and no other column names provided. This
    helps avoid ambiguity.

    Requiring __changes__ to be first argument and any other columns names that should
    be included to be supplied after.

    If the above conditions are not met, this will only return the supplied columns -
    note this means you could supply 'col1' and then '__changes__' or '__all__' if you
    happened to have a column in your table named '__changes__' or '__all__' that you
    wanted to be returned.
  */
  IF columns[1] = '__all__' THEN
    IF array_length(columns, 1) > 1 THEN
      RAISE EXCEPTION 'When subscribing to __all__ columns for a notification no other columns may be provided. Supplied array = %', columns;
    END IF;

    execute_create_fn_statement :=
        execute_create_fn_statement
            || ' IF tg_op = ''INSERT'' OR tg_op = ''UPDATE'' THEN'
            || '    notif_data := to_jsonb(new);'
            || ' ELSEIF tg_op = ''DELETE'' THEN'
            || '    notif_data := to_jsonb(old);'
            || ' ELSE'
            || '    RAISE EXCEPTION ''Unsupported trigger event for notifications.'';'
            || ' END IF;';

  ELSEIF columns[1] = '__changes__' THEN
    -- gather any extra columns provided
    FOREACH array_text IN ARRAY columns[2:] LOOP
      ret_columns := array_append(ret_columns, array_text);
    END LOOP;

    execute_create_fn_statement :=
        execute_create_fn_statement
            || ' IF tg_op = ''INSERT'' THEN '
            || '    notif_data := to_jsonb(new);'
            || ' ELSEIF tg_op = ''UPDATE'' THEN '
            || '    new_jsonb := to_jsonb(new);'
            || '    old_jsonb := to_jsonb(old);'
            || '    SELECT * INTO notif_data FROM '
            || '    ('
            || '     SELECT jsonb_object_agg(n.key, n.value) FROM '
            || '     jsonb_each(old_jsonb) as o, jsonb_each(new_jsonb) as n WHERE'
            || '     o.key = n.key AND o.value IS DISTINCT FROM n.value'
            || '    ) as a; ';

    -- this is only attached to the update case above to gather additional fields if necessary
    IF array_length(ret_columns, 1) > 0 THEN
      -- combine notif data generated above with the built json object
      execute_create_fn_statement :=
          execute_create_fn_statement || ' notif_data := notif_data || jsonb_build_object(';
      FOREACH array_text IN ARRAY ret_columns LOOP
        execute_create_fn_statement :=
            execute_create_fn_statement
                || quote_literal(array_text) || ', new_jsonb->' || quote_literal(array_text) || ', ';
      END LOOP;
      -- must remove last comma and space
      execute_create_fn_statement :=
          substring(execute_create_fn_statement, 0, length(execute_create_fn_statement) - 1)
              || '); '; -- close json builder
    END IF;

    execute_create_fn_statement :=
        execute_create_fn_statement
            || ' ELSEIF tg_op = ''DELETE'' THEN '
            || '   notif_data := to_jsonb(old);'
            || ' ELSE '
            || '    RAISE EXCEPTION ''Unsupported trigger event for notifications.'';'
            || ' END IF; ';

  ELSE
    /*
      Individual columns to return have been provided. In this case the array did
      not start with __all__ or __changes__ and if those values are provided later
      in the array they will be treated as column names in the table (resulting in
      null values for the field if they do not exists).
    */

    FOREACH array_text IN ARRAY columns LOOP
      ret_columns := array_append(ret_columns, array_text);
    END LOOP;

    /*
      Expand the column names for INSERT and UPDATE as these are the only fields
      that will be returned. The jsonb_build_object is expand here rather than
      being called and concatenated with the previously generated JSONB in a loop.
    */
    execute_create_fn_statement :=
        execute_create_fn_statement
            || 'IF tg_op = ''INSERT'' OR tg_op = ''UPDATE'' THEN '
            || '  new_jsonb := to_jsonb(new); ';

    execute_create_fn_statement :=
        execute_create_fn_statement || ' notif_data := notif_data || jsonb_build_object(';

    FOREACH array_text IN ARRAY ret_columns LOOP
      execute_create_fn_statement :=
          execute_create_fn_statement
              || quote_literal(array_text) || ', new_jsonb->' || quote_literal(array_text) || ', ';
    END LOOP;

    -- must remove last comma and space
    execute_create_fn_statement :=
        substring(execute_create_fn_statement, 0, length(execute_create_fn_statement) - 1)
            || '); '; -- close json builder

    execute_create_fn_statement :=
        execute_create_fn_statement
            || 'ELSEIF tg_op = ''DELETE'' THEN '
            || '  old_jsonb := to_jsonb(old); ';

    execute_create_fn_statement :=
        execute_create_fn_statement || ' notif_data := notif_data || jsonb_build_object(';

    FOREACH array_text IN ARRAY ret_columns LOOP
      execute_create_fn_statement :=
          execute_create_fn_statement
              || quote_literal(array_text) || ', old_jsonb->' || quote_literal(array_text) || ', ';
    END LOOP;

    -- must remove last comma and space
    execute_create_fn_statement :=
        substring(execute_create_fn_statement, 0, length(execute_create_fn_statement) - 1) || '); ';

    execute_create_fn_statement :=
        execute_create_fn_statement
            || ' ELSE '
            || '    RAISE EXCEPTION ''Unsupported trigger event for notifications.'';'
            || ' END IF; ';

  END IF;

  execute_create_fn_statement :=
      execute_create_fn_statement
          || ' notif := jsonb_build_object('
          || ' ''name'', notif_name,'
          || ' ''table'', TG_TABLE_NAME,'
          || ' ''event'', TG_OP,'
          || ' ''timestamp'', CURRENT_TIMESTAMP,'
          || ' ''data'', notif_data'
          || ' ); ';

  execute_create_fn_statement :=
      execute_create_fn_statement
          || ' PERFORM pg_notify(' || quote_literal(channelName) || ', notif::text);'
          || ' RETURN NULL; '
          || 'END; '
          || '$t$ LANGUAGE plpgsql;';

  RAISE NOTICE 'Creating function %s.', trg_fn_name;

  EXECUTE execute_create_fn_statement;
  IF exists(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = tableName::regclass AND tgname = trg_name) THEN
    RAISE NOTICE 'Trigger % already exists. Dropping trigger and recreating.', trg_name;
    EXECUTE 'DROP TRIGGER ' || quote_ident(trg_name) || ' ON ' || quote_ident(tableName);
  END IF;

  trg_event := ' AFTER ' || concat_ws(' OR ', variadic events);

  RAISE NOTICE 'Creating trigger %s on %s.', trg_name, tableName;

  EXECUTE format(
      'CREATE TRIGGER %s %s ON %s FOR EACH ROW EXECUTE PROCEDURE %s();',
      trg_name,
      trg_event,
      tableName,
      trg_fn_name
    );

END;
$a$ LANGUAGE plpgsql;

