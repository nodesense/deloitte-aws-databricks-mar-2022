### TABLE for Inserting data from Glue

```sql
CREATE SCHEMA gks;

CREATE TABLE gks.popular_movies (movieid INT, name TEXT, avg_rating DOUBLE PRECISION, total_ratings INT);

SELECT * FROM gks.popular_movies;
```
