CREATE TABLE auth_user (
    id bigserial PRIMARY KEY NOT NULL,
    username varchar(250) UNIQUE NOT NULL
);
