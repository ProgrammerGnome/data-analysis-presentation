SELECT
    f.location_name,
    f.geom AS facility_geometry,
    r.name_en AS railway_line_name,
    r.wkb_geometry AS railway_line_geometry,
    ST_Distance(f.geom, r.wkb_geometry) * 100000 AS distance_meters
FROM
    facility_locations f
JOIN railways_buildings r
    ON ST_DWithin(f.geom, r.wkb_geometry, 0.5)  -- 10 km = 0.10 fok
GROUP BY
    f.location_name, f.geom, r.name_en, r.wkb_geometry
ORDER BY
    distance_meters ASC;



CREATE TABLE facility_near_railway_buildings AS
SELECT
    f.location_name,
    f.geom AS facility_geometry,
    r.name_en AS railway_building_name,
    r.wkb_geometry AS railway_building_geometry,
    ST_Distance(f.geom, r.wkb_geometry) * 100000 AS distance_meters
FROM
    facility_locations f
JOIN railways_buildings r
    ON ST_DWithin(f.geom, r.wkb_geometry, 0.10)  -- 10 km = 0.10 fok
GROUP BY
    f.location_name, f.geom, r.name_en, r.wkb_geometry
ORDER BY
    distance_meters ASC;
