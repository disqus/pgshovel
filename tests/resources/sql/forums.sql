BEGIN;

CREATE TABLE "users" (
    id serial NOT NULL PRIMARY KEY,
    username varchar(250) NOT NULL UNIQUE
);

CREATE TABLE "profiles" (
    id serial NOT NULL PRIMARY KEY,
    user_id integer NOT NULL UNIQUE REFERENCES "users" ("id") DEFERRABLE INITIALLY DEFERRED,
    name varchar(30)
);

CREATE TABLE "forums" (
    id serial NOT NULL PRIMARY KEY,
    founder_id integer NOT NULL REFERENCES "users" ("id") DEFERRABLE INITIALLY DEFERRED,
    name varchar(30) NOT NULL
);

CREATE TABLE "threads" (
    id serial NOT NULL PRIMARY KEY,
    forum_id integer NOT NULL REFERENCES "users" ("id") DEFERRABLE INITIALLY DEFERRED,
    author_id integer NOT NULL REFERENCES "users" ("id") DEFERRABLE INITIALLY DEFERRED,
    title varchar(30) NOT NULL
);

COMMIT;
