-- Mark's homework

-- Exercise 0
-- Query all data from the specified tables to inspect their content
SELECT * FROM boundaries_lvl0;
SELECT * FROM boundaries_lvl1;
SELECT * FROM boundaries_lvl2;
SELECT * FROM railways_points;
SELECT * FROM buildings_polygons;
SELECT * FROM railways_lines;
SELECT * FROM roads_lines;
SELECT * FROM settlements_polygons;
SELECT * FROM settlements_points;

-- Exercise 1
-- Find 10 building polygons that intersect with railway points (railway stations)
-- and copy their data into a new table (railways_buildings)
CREATE TABLE railways_buildings AS
SELECT b.*
FROM buildings_polygons b
JOIN railways_points r ON ST_Intersects(b.wkb_geometry, r.wkb_geometry)
LIMIT 10;

SELECT * FROM railways_buildings;

-- Exercise 2
-- Find the railway station located near the centroid of the Pyongyang polygon (settlements_polygons.name_hun),
-- then retrieve the building_polygon related to that station, and copy the data into a new table (center_phenjan_station)
-- Search for the railway station and building near the centroid of the Pyongyang polygon
CREATE TABLE center_phenjan_station AS
WITH phenjan_centroid AS (
    SELECT ST_Centroid(s.wkb_geometry) AS centroid_geom
    FROM settlements_polygons s
    WHERE s.name_hun = 'Phenjan'
)
SELECT b.*
FROM phenjan_centroid c
JOIN railways_points r ON ST_DWithin(r.wkb_geometry, c.centroid_geom, 1000) -- assuming the nearest railway station is within 1 km
JOIN buildings_polygons b ON ST_Intersects(r.wkb_geometry, b.wkb_geometry)
ORDER BY ST_Distance(r.wkb_geometry, c.centroid_geom) -- find the nearest railway station
LIMIT 1;

SELECT * FROM center_phenjan_station;

-- Exercise 3
-- Find the settlement (only its Hungarian, English, and Korean names) that is the farthest from the railways_lines
-- and save the data into a new table (isolated_settlement)
CREATE TABLE isolated_settlement AS
SELECT s.name_hun, s.name_eng, s.name_kor
FROM settlements_polygons s
ORDER BY (
  SELECT MAX(ST_Distance(s.wkb_geometry, r.wkb_geometry))
  FROM railways_lines r
) DESC
LIMIT 1;

SELECT * FROM isolated_settlement;

-- Exercise 4
-- List all settlements that intersect with the boundaries_lvl1 line and save them into a new table (lvl1_boundaries_settlements)
CREATE TABLE lvl1_boundaries_settlements AS
SELECT s.*
FROM settlements_polygons s
JOIN boundaries_lvl1 b ON ST_Intersects(s.wkb_geometry, b.wkb_geometry);

SELECT * FROM lvl1_boundaries_settlements;
