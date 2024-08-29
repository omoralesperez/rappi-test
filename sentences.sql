CREATE OR REPLACE TABLE dim_categories (
    id_category NUMBER AUTOINCREMENT, -- Clave subrogada con auto incremento
    category_id NUMBER,               -- ID original de la categoría
    category_name STRING,             -- Nombre de la categoría
    shortname STRING,                 -- Nombre corto de la categoría
    sort_name STRING,                 -- Nombre para ordenar
    PRIMARY KEY (id_category)         -- Clave primaria
);




------


CREATE OR REPLACE TABLE STAGE_DB.STAGE_SCHEMA.TEMP_FCT_MEETUP (
    ID_FCT_MEETUP NUMBER(38,0) AUTOINCREMENT,
    GROUP_ID NUMBER(38,0),
    GROUP_NAME VARCHAR(16777216),
    GROUP_DESCRIPTION VARCHAR(16777216),
    CREATED TIMESTAMP_NTZ(9),
    ORGANIZER_MEMBER_ID NUMBER(38,0),
    ORGANIZER_NAME VARCHAR(16777216),
    CITY_ID NUMBER(38,0),
    CITY VARCHAR(16777216),
    EVENT_ID VARCHAR(16777216),
    EVENT_NAME VARCHAR(16777216),
    CATEGORY_ID VARCHAR(16777216),
    CATEGORY_NAME VARCHAR(16777216),
    MAIN_TOPIC_ID NUMBER(38,0),
    TOPIC_NAME VARCHAR(16777216),
    MEMBERS NUMBER(38,0),
    RATING FLOAT,
    DURATION NUMBER(38,0)
);



CREATE TABLE IF NOT EXISTS temp_cities (
    city VARCHAR,
    city_id INTEGER,
    country VARCHAR,
    distance FLOAT,
    latitude FLOAT,
    localized_country_name VARCHAR,
    longitude FLOAT,
    member_count INTEGER,
    ranking INTEGER,
    state VARCHAR,
    zip INTEGER
);

CREATE TABLE IF NOT EXISTS temp_events (
    event_id VARCHAR,
    created TIMESTAMP,
    description TEXT,
    duration INTEGER,
    event_url VARCHAR,
    fee_accepts VARCHAR,
    fee_amount FLOAT,
    fee_currency VARCHAR,
    fee_description VARCHAR,
    fee_label VARCHAR,
    fee_required INTEGER,
    group_created TIMESTAMP,
    group_group_lat FLOAT,
    group_group_lon FLOAT,
    group_id INTEGER,
    group_join_mode VARCHAR,
    group_name VARCHAR,
    group_urlname VARCHAR,
    group_who VARCHAR,
    headcount INTEGER,
    how_to_find_us TEXT,
    maybe_rsvp_count INTEGER,
    event_name VARCHAR,
    photo_url VARCHAR,
    rating_average FLOAT,
    rating_count INTEGER,
    rsvp_limit INTEGER,
    event_status VARCHAR,
    event_time TIMESTAMP,
    updated TIMESTAMP,
    utc_offset INTEGER,
    venue_address_1 VARCHAR,
    venue_address_2 VARCHAR,
    venue_city VARCHAR,
    venue_country VARCHAR,
    venue_id INTEGER,
    venue_lat FLOAT,
    venue_localized_country_name VARCHAR,
    venue_lon FLOAT,
    venue_name VARCHAR,
    venue_phone INTEGER,
    venue_repinned INTEGER,
    venue_state VARCHAR,
    venue_zip INTEGER,
    visibility VARCHAR,
    waitlist_count INTEGER,
    why TEXT,
    yes_rsvp_count INTEGER
);

CREATE TABLE IF NOT EXISTS temp_groups (
    group_id INTEGER,
    category_id INTEGER,
    category_name VARCHAR,
    category_shortname VARCHAR,
    city_id INTEGER,
    city VARCHAR,
    country VARCHAR,
    created TIMESTAMP,
    description TEXT,
    group_photo_base_url VARCHAR,
    group_photo_highres_link VARCHAR,
    group_photo_photo_id INTEGER,
    group_photo_photo_link VARCHAR,
    group_photo_thumb_link VARCHAR,
    group_photo_type VARCHAR,
    join_mode VARCHAR,
    lat FLOAT,
    link VARCHAR,
    lon FLOAT,
    members INTEGER,
    group_name VARCHAR,
    organizer_member_id INTEGER,
    organizer_name VARCHAR,
    organizer_photo_base_url VARCHAR,
    organizer_photo_highres_link VARCHAR,
    organizer_photo_photo_id INTEGER,
    organizer_photo_photo_link VARCHAR,
    organizer_photo_thumb_link VARCHAR,
    organizer_photo_type VARCHAR,
    rating FLOAT,
    state VARCHAR,
    timezone VARCHAR,
    urlname VARCHAR,
    utc_offset INTEGER,
    visibility VARCHAR,
    who VARCHAR
);

CREATE TABLE IF NOT EXISTS temp_groups_topics (
    topic_id INTEGER,
    topic_key VARCHAR,
    topic_name VARCHAR,
    group_id INTEGER
);

CREATE TABLE IF NOT EXISTS temp_topics (
    topic_id INTEGER,
    description TEXT,
    link VARCHAR,
    members INTEGER,
    topic_name VARCHAR,
    urlkey VARCHAR,	
    main_topic_id INTEGER
);

--- sp creado en snowflake

CREATE OR REPLACE PROCEDURE LOAD_STAGE_DATA()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN

  -- Copy into TEMP_CITIES
  COPY INTO "STAGE_DB"."STAGE_SCHEMA"."TEMP_CITIES" 
  FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
    FROM '@"STAGE_DB"."STAGE_SCHEMA"."STAGE_TEST_RAPPI"'
  )
  FILES = ('DATA_INPUT/cities.csv') 
  FILE_FORMAT = 'STAGE_DB.STAGE_SCHEMA.LATIN1_CSV_FORMAT' 
  ON_ERROR = CONTINUE;

  -- Copy into TEMP_EVENTS
  COPY INTO "STAGE_DB"."STAGE_SCHEMA"."TEMP_EVENTS" 
  FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48
    FROM '@"STAGE_DB"."STAGE_SCHEMA"."STAGE_TEST_RAPPI"'
  )
  FILES = ('DATA_INPUT/events.csv') 
  FILE_FORMAT = 'STAGE_DB.STAGE_SCHEMA.LATIN1_CSV_FORMAT' 
  ON_ERROR = CONTINUE;

  -- Copy into TEMP_GROUPS
  COPY INTO "STAGE_DB"."STAGE_SCHEMA"."TEMP_GROUPS" 
  FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36
    FROM '@"STAGE_DB"."STAGE_SCHEMA"."STAGE_TEST_RAPPI"'
  )
  FILES = ('DATA_INPUT/groups.csv') 
  FILE_FORMAT = 'STAGE_DB.STAGE_SCHEMA.LATIN1_CSV_FORMAT' 
  ON_ERROR = CONTINUE;

  -- Copy into GROUPS_TOPICS
  COPY INTO "STAGE_DB"."STAGE_SCHEMA"."TEMP_GROUPS_TOPICS" 
  FROM (
    SELECT $1, $2, $3, $4
    FROM '@"STAGE_DB"."STAGE_SCHEMA"."STAGE_TEST_RAPPI"'
  )
  FILES = ('DATA_INPUT/groups_topics.csv') 
  FILE_FORMAT = 'STAGE_DB.STAGE_SCHEMA.LATIN1_CSV_FORMAT' 
  ON_ERROR = CONTINUE;

  -- Copy into TEMP_CATEGORIES
  COPY INTO "STAGE_DB"."STAGE_SCHEMA"."TEMP_CATEGORIES" 
  FROM (
    SELECT $1, $2, $3, $4
    FROM '@"STAGE_DB"."STAGE_SCHEMA"."STAGE_TEST_RAPPI"'
  )
  FILES = ('DATA_INPUT/categories.csv') 
  FILE_FORMAT = 'STAGE_DB.STAGE_SCHEMA.LATIN1_CSV_FORMAT' 
  ON_ERROR = CONTINUE;

  -- Commit the transaction
  COMMIT;

  RETURN 'Data load completed successfully.';

END;
$$;

-- Crear la tabla 'lote' con una columna autoincremental
CREATE OR REPLACE TABLE temp_lote (
    id_lote INT AUTOINCREMENT START 1 INCREMENT 1,
    periodo DATE
);

-- Insertar los datos en la tabla 'lote' (sin especificar 'id_lote', ya que es autoincremental)
INSERT INTO temp_lote (periodo)
VALUES ('2012-01-01');

