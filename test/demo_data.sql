

DROP SCHEMA IF EXISTS pirogue_test CASCADE;

CREATE SCHEMA pirogue_test;

CREATE SEQUENCE pirogue_test.id_gen START 101;

CREATE OR REPLACE FUNCTION pirogue_test.generate_id(table_name text)
  RETURNS integer AS
$BODY$
DECLARE
BEGIN
    RETURN 1000 + nextval('pirogue_test.id_gen');
END;
$BODY$
LANGUAGE plpgsql;

CREATE TABLE pirogue_test.animal (
	aid integer PRIMARY KEY default pirogue_test.generate_id('animal'),
	name text,
	year smallint);

CREATE TABLE pirogue_test.cat_breed ( id integer PRIMARY KEY, breed_name text );
CREATE TABLE pirogue_test.dog_breed ( id integer PRIMARY KEY, breed_name text );
CREATE TABLE pirogue_test.vet ( id integer PRIMARY KEY, vet_name text );

CREATE TABLE pirogue_test.cat (
	cid integer REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.cat_breed,
	fk_vet integer REFERENCES pirogue_test.vet,
	eye_color text); -- not in top class, as some animals might not have eyes

CREATE TABLE pirogue_test.dog (
	did integer REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.dog_breed,
	eye_color text);

-- ref col has same name as parent pkey column
CREATE TABLE pirogue_test.aardvark (
   	aid integer REFERENCES pirogue_test.animal,
	father text
	-- ! if adding new fields here, complete the list on test_multiple_inheritance.test_merge_no_columns ! --
	);

-- ref col is a serial key
CREATE TABLE pirogue_test.eagle (
   	eid serial PRIMARY KEY,
   	fk_animal integer REFERENCES pirogue_test.animal,
	weight double precision);


INSERT INTO pirogue_test.cat_breed (id, breed_name) VALUES (1, 'Abyssinian');
INSERT INTO pirogue_test.cat_breed (id, breed_name) VALUES (2, 'American Bobtail');
INSERT INTO pirogue_test.dog_breed (id, breed_name) VALUES (1, 'Afghan Hound');
INSERT INTO pirogue_test.dog_breed (id, breed_name) VALUES (2, 'Barbet');