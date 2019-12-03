CREATE TABLE IF NOT EXISTS d_date
(
    id      SERIAL PRIMARY KEY,
    date    TIMESTAMP NOT NULL UNIQUE,
    year    SMALLINT CHECK (year >= 0),
    quarter SMALLINT CHECK (quarter >= 1 AND quarter <= 4),
    month   SMALLINT CHECK (month >= 1 AND month <= 12),
    day     SMALLINT CHECK (day >= 1 AND day <= 31)
);

CREATE OR REPLACE FUNCTION date_parts() RETURNS TRIGGER AS
$$
BEGIN
    UPDATE d_date
    SET year    = date_part('year', NEW.date),
        quarter = date_part('quarter', NEW.date),
        month   = date_part('month', NEW.date),
        day     = date_part('day', NEW.date)
    WHERE date = NEW.date;

    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER date_parts
    AFTER INSERT
    ON d_date
    FOR EACH ROW
EXECUTE PROCEDURE date_parts();

CREATE TABLE IF NOT EXISTS d_genre
(
    id    SERIAL PRIMARY KEY,
    title TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_age_category
(
    id    SERIAL PRIMARY KEY,
    adult BOOLEAN NOT NULL UNIQUE
);

INSERT INTO d_age_category(adult)
VALUES (TRUE),
       (FALSE)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS d_movie_franchise
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_studio
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS d_language
(
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL UNIQUE,
    iso_639_1 TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_country
(
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    iso_3166_1 TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_keyword
(
    id   INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS d_movie
(
    id                   SERIAL PRIMARY KEY,
    id_age_category      INT REFERENCES d_age_category (id),
    id_movie_franchise   INT REFERENCES d_movie_franchise (id),
    id_original_language INT REFERENCES d_language (id),
    original_title       TEXT NOT NULL,
    title                TEXT NOT NULL,
    imdb_id              TEXT
);

CREATE TABLE IF NOT EXISTS movie_genres
(
    id_movie INT REFERENCES d_movie (id),
    id_genre INT REFERENCES d_genre (id)
);

CREATE TABLE IF NOT EXISTS production_studios
(
    id_movie  INT REFERENCES d_movie (id),
    id_studio INT REFERENCES d_studio (id)
);

CREATE TABLE IF NOT EXISTS production_countries
(
    id_movie   INT REFERENCES d_movie (id),
    id_country INT REFERENCES d_country (id)
);

CREATE TABLE IF NOT EXISTS spoken_languages
(
    id_movie    INT REFERENCES d_movie (id),
    id_language INT REFERENCES d_language (id)
);

CREATE TABLE IF NOT EXISTS movie_keywords
(
    id_movie   INT REFERENCES d_movie (id),
    id_keyword INT REFERENCES d_keyword (id)
);

CREATE TABLE IF NOT EXISTS d_gender
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

INSERT INTO d_gender(name)
VALUES ('Unspecified'),
       ('Female'),
       ('Male')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS d_character
(
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_people
(
    id        SERIAL PRIMARY KEY,
    id_gender INT REFERENCES d_gender (id),
    name      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS d_department
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_job
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_cast
(
    id_movie     INT REFERENCES d_movie (id),
    id_character INT REFERENCES d_character (id),
    id_people    INT REFERENCES d_people (id)
);

CREATE TABLE IF NOT EXISTS d_crew
(
    id_movie      INT REFERENCES d_movie (id),
    id_department INT REFERENCES d_department (id),
    id_job        INT REFERENCES d_job (id),
    id_people     INT REFERENCES d_people (id)
);

CREATE TABLE IF NOT EXISTS d_release_status
(
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS d_user
(
    id            SERIAL PRIMARY KEY,
    abstract_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS f_movie_popularity
(
    id_date           INT REFERENCES d_date (id),
    id_movie          INT REFERENCES d_movie (id),
    id_release_status INT REFERENCES d_release_status (id),
    id_user           INT REFERENCES d_user (id),
    f_rating          REAL CHECK (f_rating IS NULL OR (f_rating >= 0 AND f_rating <= 5)),
    f_budget          INT CHECK (f_budget IS NULL OR f_budget >= 0),
    f_revenue         INT,
    f_runtime         REAL CHECK (f_runtime IS NULL OR f_runtime >= 0),
    f_popularity      REAL
);
