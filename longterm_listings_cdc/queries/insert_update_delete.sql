-- ============================================================
-- SQL Queries for long_term_listings table
-- Schema: berlin_source_data
-- Purpose: Simulate INSERT, UPDATE, DELETE operations
-- ============================================================
-- ============================================================
-- INSERT: Add a new listing
-- ============================================================
INSERT INTO berlin_source_data.long_term_listings (
        id,
        name,
        type,
        first_tenant,
        price_euro,
        number_of_rooms,
        surface_m2,
        floor,
        street,
        house_number,
        neighborhood,
        district,
        postal_code,
        address,
        latitude,
        longitude,
        geometry,
        district_id,
        neighborhood_id
    )
VALUES (
        'test_listing_001',
        'Wohnung zur Miete 1200 € 2 Zimmer 55 m² Mitte Berlin 10115',
        'Wohnung',
        'no',
        1200.00,
        2.0,
        55.0,
        '3',
        'Unter den Linden',
        '10',
        'Mitte',
        'Mitte',
        '10115',
        '10115 Mitte Berlin',
        52.516275,
        13.377704,
        'POINT(13.377704 52.516275)',
        '11001001',
        '0101'
    );
-- ============================================================
-- UPDATE: Modify price and floor for the listing
-- ============================================================
UPDATE berlin_source_data.long_term_listings
SET price_euro = 1350.00,
    floor = '4',
    first_tenant = 'yes'
WHERE id = 'test_listing_001';
-- ============================================================
-- DELETE: Remove a listing
-- ============================================================
DELETE FROM berlin_source_data.long_term_listings
WHERE id = 'test_listing_001';
-- ============================================================
-- ANALYSIS: Most likely to change 
-- price_euro       - changes often (market fluctuations)
-- first_tenant     - changes when new tenant moves in
-- floor            - rarely changes
-- name             - rarely changes
-- id               - never changes (primary key)
-- ============================================================