-- Recordings for each ISRC, each with the album to file it under (aurral-shim, estate fork).
-- $1: ISRCs. $2: album titles in the same order, '' where there is none (Deezer's album).
-- The album is the earliest official Album with no secondary type, credited to an artist of
-- the recording, that contains it: the rule the estate used to move songs off compilations.
-- Failing that, the official release group whose title matches the given album title.
-- Within the chosen release group, the earliest release that has the recording gives the
-- release ID, disc and track number. A recording on no such release group comes back with
-- its album columns empty.
WITH input AS (
    -- isrc.isrc is char(12): compared as text, its index is not used.
    SELECT i.isrc::char(12) AS isrc,
           lower(regexp_replace(coalesce(i.album, ''), '[^[:alnum:]]+', '', 'g')) AS album_key
    FROM unnest($1::text[], $2::text[]) AS i(isrc, album)
),
-- LATERAL lookups keep every step on its index: the planner cannot tell how few ISRCs come
-- in, and with plain joins it hashed the whole release and release group tables (13 s).
-- OFFSET 0 stops it flattening each LATERAL back into those joins.
recs AS MATERIALIZED (
    SELECT DISTINCT input.isrc, input.album_key,
           found.rid, found.recording_id, found.title, found.length, found.artist_credit
    FROM input
    CROSS JOIN LATERAL (
        SELECT recording.id AS rid, recording.gid AS recording_id, recording.name AS title,
               recording.length, recording.artist_credit
        FROM isrc
        JOIN recording ON recording.id = isrc.recording
        WHERE isrc.isrc = input.isrc
        OFFSET 0
    ) found
),
placements AS (
    SELECT recs.rid, placed.*
    FROM recs
    CROSS JOIN LATERAL (
    SELECT release_group.gid AS release_group_id,
           release_group.name AS album_title,
           release_group.artist_credit AS album_credit,
           release.id AS release_row,
           release.gid AS release_id,
           medium.position AS disc,
           track.position AS track,
           meta.first_release_date_year AS y,
           meta.first_release_date_month AS m,
           meta.first_release_date_day AS d,
           event.date_year AS ry, event.date_month AS rm, event.date_day AS rd,
           (release_group.type = 1
            AND NOT EXISTS (SELECT 1 FROM release_group_secondary_type_join sec
                            WHERE sec.release_group = release_group.id)
            AND EXISTS (SELECT 1
                        FROM artist_credit_name album_artist
                        JOIN artist_credit_name song_artist ON song_artist.artist = album_artist.artist
                        WHERE album_artist.artist_credit = release_group.artist_credit
                          AND song_artist.artist_credit = recs.artist_credit)) AS rule_album,
           (recs.album_key <> ''
            AND lower(regexp_replace(release_group.name, '[^[:alnum:]]+', '', 'g')) = recs.album_key) AS title_match
    FROM track
    JOIN medium ON medium.id = track.medium
    JOIN release ON release.id = medium.release AND release.status = 1
    JOIN release_group ON release_group.id = release.release_group
    LEFT JOIN release_group_meta meta ON meta.id = release_group.id
    LEFT JOIN LATERAL (
        SELECT date_year, date_month, date_day FROM release_event
        WHERE release_event.release = release.id
        ORDER BY date_year NULLS LAST, date_month NULLS LAST, date_day NULLS LAST
        LIMIT 1
    ) event ON true
    WHERE track.recording = recs.rid
    OFFSET 0
    ) placed
),
chosen AS (
    SELECT DISTINCT ON (rid) *
    FROM (
        SELECT 1 AS priority, placements.* FROM placements WHERE rule_album
        UNION ALL
        SELECT 2 AS priority, placements.* FROM placements WHERE title_match
    ) candidates
    ORDER BY rid, priority, y NULLS LAST, m NULLS LAST, d NULLS LAST,
             ry NULLS LAST, rm NULLS LAST, rd NULLS LAST, release_row
)
SELECT recs.isrc,
       recs.recording_id,
       recs.title,
       recs.length,
       (SELECT name FROM artist_credit WHERE id = recs.artist_credit) AS artist,
       (SELECT artist.gid FROM artist_credit_name acn JOIN artist ON artist.id = acn.artist
        WHERE acn.artist_credit = recs.artist_credit ORDER BY acn.position LIMIT 1) AS artist_id,
       chosen.release_group_id,
       chosen.album_title,
       (SELECT name FROM artist_credit WHERE id = chosen.album_credit) AS album_artist,
       (SELECT artist.gid FROM artist_credit_name acn JOIN artist ON artist.id = acn.artist
        WHERE acn.artist_credit = chosen.album_credit ORDER BY acn.position LIMIT 1) AS album_artist_id,
       CASE WHEN chosen.y IS NULL THEN NULL
            ELSE chosen.y::text || coalesce('-' || lpad(chosen.m::text, 2, '0'), '')
                                || coalesce('-' || lpad(chosen.d::text, 2, '0'), '')
       END AS first_release_date,
       chosen.release_id,
       chosen.disc,
       chosen.track,
       CASE chosen.priority WHEN 1 THEN 'rule' WHEN 2 THEN 'title' END AS album_match
FROM recs
LEFT JOIN chosen ON chosen.rid = recs.rid
ORDER BY recs.isrc, recs.recording_id
