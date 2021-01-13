/*
  This should be called after notifications.sql has been setup. This way
  the table can easily be dropped and restored for test values if
  desired.
*/

CREATE TABLE IF NOT EXISTS test_notifs (
  id serial,
  col1 integer,
  col2 numeric,
  col3 text
);

INSERT INTO test_notifs (col1, col2, col3) VALUES (1, 1.23, 'one');
INSERT INTO test_notifs (col1, col2, col3) VALUES (2, 4.56, 'two');
INSERT INTO test_notifs (col1, col2, col3) VALUES (3, 7.89, 'three');

-- basically same table, just different name for testing

CREATE TABLE IF NOT EXISTS test_notifs_2 (
  id serial,
  col1 integer,
  col2 numeric,
  col3 text
);

INSERT INTO test_notifs_2 (col1, col2, col3) VALUES (1, 1.23, 'one');
INSERT INTO test_notifs_2 (col1, col2, col3) VALUES (2, 4.56, 'two');
INSERT INTO test_notifs_2 (col1, col2, col3) VALUES (3, 7.89, 'three');
