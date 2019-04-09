

DROP SCHEMA IF EXISTS pirogue_test CASCADE;

CREATE SCHEMA pirogue_test;

CREATE SEQUENCE pirogue_test.id_gen START 101;

CREATE OR REPLACE FUNCTION pirogue_test.generate_id(table_name text)
  RETURNS text AS
$BODY$
DECLARE
BEGIN
    RETURN table_name || '_' || nextval('pirogue_test.id_gen');
END;
$BODY$
LANGUAGE plpgsql;

CREATE TABLE pirogue_test.animal (
	id text PRIMARY KEY default pirogue_test.generate_id('animal'),
	name text,
	year smallint);

CREATE TABLE pirogue_test.cat_breed ( id integer PRIMARY KEY, name text );
CREATE TABLE pirogue_test.dog_breed ( id integer PRIMARY KEY, name text );

CREATE TABLE pirogue_test.cat (
	cid text REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.cat_breed,
	eye_color text); -- not in top class, as some animals might not have eyes

CREATE TABLE pirogue_test.dog (
	did text REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.dog_breed,
	eye_color text);


INSERT INTO pirogue_test.cat_breed (id, name) VALUES (1, 'Abyssinian');
INSERT INTO pirogue_test.cat_breed (id, name) VALUES (2, 'American Bobtail');
INSERT INTO pirogue_test.dog_breed (id, name) VALUES (1, 'Afghan Hound');
INSERT INTO pirogue_test.dog_breed (id, name) VALUES (2, 'Barbet');