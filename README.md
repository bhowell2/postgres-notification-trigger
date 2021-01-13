# Postgres Notification Trigger
Provides the functionality to create a trigger on a table that will send a notification to the given channel 
when a subscribed event (insert, update, or delete) occurs on the table. This also allows for specifying which
columns are returned with the notification. The notifications are sent with `pg_notify`
(See [docs](https://www.postgresql.org/docs/current/sql-notify.html) for additional information on `LISTEN/NOTIFY`).

This is simply provided by copy-paste! 

**Supports Postgres Version >= 9.5**

## Usage
The recommended way to use this is with the entire [`notifications.sql`](./notifications.sql) schema; which 
provides a table that can be modified to create/update/delete the notification triggers. This allows the user to 
easily view which notification triggers exists throughout the database. However, the alternative is to use only 
the function `fn_create_notification_trigger`, which will create the same trigger as inserting into the `trg_notifs` table (which calls `fn_create_notification_trigger` itself).

```javascript
{
  "name": notif_name, // may be null 
  "table": tg_table_name, // table for which the notification was generated
  "event": tg_op, // INSERT, UPDATE, DELETE
  "timestamp": CURRENT_TIMESTAMP, // timestamp when transaction of event occurred
    "data": { // whatever columns (__all__, __changes__, or specific columns) were subscribed to for events
      "col1": "col1 data"
      //...
      "colN": "colN data"
  }
}
```

|column|type|description|
|------|----|-----------|
| id | serial | Primary Key. Don't need to insert yourself unless you're a savage like that. |
| table_name | text | Name of the table for which the notification trigger should be created. Cannot be null. |
| channel_name | text | Name of the channel that will be notified of the event. Cannot be null. |
| notif_name | text | Name of the notification. Will be sent in the payload. May be null. |
| columns | text[] | The column names that will be sent in the 'data' of the notification. This may be '{\_\_all\_\_}', '{\_\_changes\_\_}', or '{col1, col2, ... colN}'. There is one more possible value and that is '{\_\_changes\_\_, col1, col2,...} which will send any column that changed and also the extra provided columns. |
| events | text[] | The events (insert, update, or delete) for which the notification should be sent. (E.g., '{insert, update}.)'|
| tg_fn_name | text | This is generated. Don't set yourself. |
| trg_name | text | This is generated. Don't set yourself. |


## Performance
The notification is done by a trigger that creates the required notification JSON. This will have some performance 
impact, but I do not think it will be very noticeable unless you have a very write-heavy workload and I don't 
think this would be the best solution in that case as your DB would spend a significant amount of time sending out 
notifications...
