

DROP SCHEMA IF EXISTS pirogue_test CASCADE;

CREATE SCHEMA pirogue_test;

CREATE TABLE pirogue_test.animal (
	id serial PRIMARY KEY,
	name text,
	year smallint);

CREATE TABLE pirogue_test.cat_breed ( id integer PRIMARY KEY, name text );
CREATE TABLE pirogue_test.dog_breed ( id integer PRIMARY KEY, name text );

CREATE TABLE pirogue_test.cat (
	id integer REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.cat_breed,
	eye_color text); -- not in top class, as some animals might not have eyes

CREATE TABLE pirogue_test.dog (
	id integer REFERENCES pirogue_test.animal,
	fk_breed integer REFERENCES pirogue_test.dog_breed,
	eye_color text);


INSERT INTO pirogue_test.cat_breed (id, name) VALUES (1, 'Abyssinian');
INSERT INTO pirogue_test.cat_breed (id, name) VALUES (2, 'American Bobtail');
INSERT INTO pirogue_test.dog_breed (id, name) VALUES (1, 'Afghan Hound');
INSERT INTO pirogue_test.dog_breed (id, name) VALUES (2, 'Barbet');