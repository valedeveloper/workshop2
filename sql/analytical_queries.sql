-- KPI 1: percentage of distinct Spotify artists with an observed Grammy winner record.
WITH spotify_artists AS (
    SELECT DISTINCT artist_sk
    FROM public.fact_spotify_artist_track_genre
    WHERE artist_sk IS NOT NULL
),
grammy_winners AS (
    SELECT DISTINCT artist_sk
    FROM public.fact_music_award
    WHERE artist_sk IS NOT NULL
      AND winner IS TRUE
)
SELECT
    COUNT(*) AS spotify_artists,
    COUNT(*) FILTER (WHERE grammy_winners.artist_sk IS NOT NULL)
        AS spotify_artists_with_grammy_win,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE grammy_winners.artist_sk IS NOT NULL
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS pct_spotify_artists_with_grammy_win
FROM spotify_artists
LEFT JOIN grammy_winners USING (artist_sk);

-- KPI 2: average popularity by genre, counting each song only once per genre.
WITH song_genre AS (
    SELECT
        genre_sk,
        track_sk,
        MAX(popularity) AS popularity
    FROM public.fact_spotify_artist_track_genre
    GROUP BY genre_sk, track_sk
)
SELECT
    genre.genre_name,
    AVG(song_genre.popularity) AS average_popularity,
    COUNT(*) AS song_count
FROM song_genre
JOIN public.dim_genero AS genre
    ON genre.genre_sk = song_genre.genre_sk
GROUP BY genre.genre_name
ORDER BY average_popularity DESC, genre.genre_name;

-- KPI 3: top five genres by observed Grammy winner records for their artists.
WITH genre_artists AS (
    SELECT DISTINCT genre_sk, artist_sk
    FROM public.fact_spotify_artist_track_genre
    WHERE artist_sk IS NOT NULL
)
SELECT
    genre.genre_name,
    SUM(award.award_count) AS observed_grammy_winner_records,
    COUNT(DISTINCT genre_artists.artist_sk) AS spotify_artists_with_wins
FROM genre_artists
JOIN public.fact_music_award AS award
    ON award.artist_sk = genre_artists.artist_sk
   AND award.winner IS TRUE
JOIN public.dim_genero AS genre
    ON genre.genre_sk = genre_artists.genre_sk
GROUP BY genre.genre_name
ORDER BY observed_grammy_winner_records DESC, genre.genre_name
LIMIT 5;

-- KPI 4: average song popularity by artist, grouped by observed Grammy winner status.
WITH song_genre AS (
    SELECT
        track_sk,
        genre_sk,
        MAX(popularity) AS popularity
    FROM public.fact_spotify_artist_track_genre
    GROUP BY track_sk, genre_sk
),
song_popularity AS (
    SELECT track_sk, AVG(popularity) AS average_song_popularity
    FROM song_genre
    GROUP BY track_sk
),
artist_tracks AS (
    SELECT DISTINCT
        credit.artist_sk,
        credit.track_sk,
        song_popularity.average_song_popularity
    FROM public.fact_spotify_artist_track_genre AS credit
    JOIN song_popularity
        ON song_popularity.track_sk = credit.track_sk
    WHERE credit.artist_sk IS NOT NULL
),
artist_grammy_status AS (
    SELECT
        artist_tracks.artist_sk,
        AVG(artist_tracks.average_song_popularity)
            AS average_artist_popularity,
        BOOL_OR(award.winner IS TRUE) AS has_observed_grammy_win
    FROM artist_tracks
    LEFT JOIN public.fact_music_award AS award
        ON award.artist_sk = artist_tracks.artist_sk
    GROUP BY artist_tracks.artist_sk
)
SELECT
    CASE
        WHEN has_observed_grammy_win THEN 'Con registro Grammy ganador'
        ELSE 'Sin registro Grammy ganador'
    END AS grammy_group,
    AVG(average_artist_popularity) AS average_popularity_per_artist,
    COUNT(*) AS artist_count
FROM artist_grammy_status
GROUP BY has_observed_grammy_win
ORDER BY has_observed_grammy_win DESC;
