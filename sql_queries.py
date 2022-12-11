# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (   
                                songplay_id SERIAL PRIMARY KEY,
                                start_time timestamp NOT NULL,
                                user_id int NOT NULL,
                                level varchar,
                                song_id varchar,
                                artist_id varchar,
                                session_id varchar NOT NULL,
                                location varchar,
                                user_agent text NOT NULL
                            )
""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                            user_id int,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            gender varchar,
                            level varchar,
                            PRIMARY KEY (user_id)
                        )
""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id varchar,
                            title varchar ,
                            artist_id varchar NOT NULL,
                            year int,
                            duration float,
                            PRIMARY KEY (song_id)
                        )
""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                          (
                              artist_id varchar,
                              name varchar NOT NULL,
                              location varchar,
                              latitude float,
                              longitude float,
                              PRIMARY KEY (artist_id)
                          )
""")


time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                            start_time timestamp,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday int, 
                            PRIMARY KEY (start_time)
                        )
""")


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays
                            (
                                start_time,
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent
                            )
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
""")


user_table_insert = ("""INSERT INTO users
                        (
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        )
                     VALUES (%s, %s, %s, %s, %s) 
                     ON CONFLICT (user_id)
                     DO UPDATE SET level = EXCLUDED.level
""")


song_table_insert = ("""INSERT INTO songs
                        (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        )
                     VALUES (%s, %s, %s, %s, %s)
                     ON CONFLICT (song_id) DO NOTHING
""")


artist_table_insert = ("""INSERT INTO artists
                          (
                              artist_id,
                              name,
                              location,
                              latitude,
                              longitude
                          )
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT INTO time
                        (
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday
                        )
                     VALUES (%s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT (start_time) DO NOTHING
""")


# BULK INSERT RECORDS

# songplay_table_bulk_insert = ("""COPY songplays FROM stdin WITH CSV HEADER DELIMITER as ','""")

songplay_table_bulk_insert = ("""CREATE TABLE IF NOT EXISTS records
                                 (
                                     start_time timestamp NOT NULL,
                                     user_id int,
                                     level varchar,
                                     session_id varchar,
                                     location varchar,
                                     user_agent text,
                                     song text
                                 );
                                 
                                 COPY records FROM stdin WITH CSV HEADER
                                 DELIMITER as ',';
                                 
                                 INSERT INTO songplays
                                (
                                    start_time,
                                    user_id,
                                    level,
                                    song_id,
                                    artist_id,
                                    session_id,
                                    location,
                                    user_agent
                                )
                                
                                SELECT records.start_time, records.user_id, records.level, songs.song_id, songs.artist_id,
                                       records.session_id, records.location, records.user_agent 
                                FROM (records LEFT OUTER JOIN songs ON records.song = songs.title);
                                
                                DROP TABLE records;         
                                 
""")

user_table_bulk_insert = ("""COPY users FROM stdin WITH CSV HEADER DELIMITER as ','""")


# Another solution to bulk insert data with possible conflicts
# It creates a temporary table with no primary key constrains (or could be modified to remove specific contrains from the desired column)
# Then it copies the data from that table which allows the usage of "ON CONFLICT" becuase it is not currently widley availible when using the "COPY" command
# This solution is useful when there are no more than 1 conflict per single row as it is not currently supported to handle many bulk insert conflicts for the same row
# In this case the user table has could have multiple conflicts per row so the data processing will be handeles on the main server by python

"""user_table_bulk_insert = (CREATE TABLE IF NOT EXISTS temp_users
                            (
                                user_id int,
                                first_name varchar NOT NULL,
                                last_name varchar NOT NULL,
                                gender varchar,
                                level varchar
                            );
                            
                            COPY temp_users FROM stdin WITH CSV HEADER
                            DELIMITER as ',';
                            
                            INSERT INTO users
                            (
                                user_id,
                                first_name,
                                last_name,
                                gender,
                                level
                            )
                            SELECT *
                            FROM temp_users ON conflict (user_id) 
                            DO UPDATE SET level = EXCLUDED.level;

                            DROP TABLE temp_users;
)"""


song_table_bulk_insert = ("""COPY songs FROM stdin WITH CSV HEADER DELIMITER as ','""")


artist_table_bulk_insert = ("""COPY artists FROM stdin WITH CSV HEADER DELIMITER as ','""")


# Another solution to bulk insert data with possible conflicts
# It creates a temporary table with no primary key constrains (or could be modified to remove specific contrains from the desired column)
# Then it copies the data from that table which allows the usage of "ON CONFLICT" becuase it is not currently widley availible when using the "COPY" command
# This solution is useful when there are no more than 1 conflict per single row as it is not currently supported to handle many bulk insert conflicts for the same row
# It can be implemented with the time table because data would very rarely 1 conflict per row (start_time) at the current scale of the bussiness
# The cleaning could do the same and we could use a simple query but sql handles data processing better than python
# So by using this query we divert some of the processing load off the main hosting server to the database hosting service which handles data processing better
# REMEMBER if the scale of the app users increases significantly then the probability of having more than 1 conflicted timestamps down to the millisecond becomes more likley

time_table_bulk_insert = ("""CREATE TABLE IF NOT EXISTS temp_time
                            (
                                start_time timestamp,
                                hour int,
                                day int,
                                week int,
                                month int,
                                year int,
                                weekday int 
                            );
                            
                            COPY temp_time FROM stdin WITH CSV HEADER
                            DELIMITER as ',';
                            
                            INSERT INTO time
                            (
                                start_time,
                                hour,
                                day,
                                week,
                                month,
                                year,
                                weekday
                            )
                            SELECT *
                            FROM temp_time ON conflict (start_time) 
                            DO NOTHING;

                            DROP TABLE temp_time;
""")


# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id
                  FROM songs
                  INNER JOIN artists
                  ON songs.artist_id=artists.artist_id
                  WHERE songs.title = %s
                  AND artists.name = %s
                  AND songs.duration = %s
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]