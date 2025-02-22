CREATE VIEW dim_delivery_location AS
SELECT
    location_key AS delivery_location_key,
    location AS delivery_location
FROM dim_locations;

CREATE VIEW dim_pick_up_location AS
SELECT
    location_key AS pick_up_location_key,
    location AS pick_up_location
FROM dim_locations;
