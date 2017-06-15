/* 
 * CFCWM database for Weather Data Store - insert_curw_data.sql.
 * You need to run this script with an authorized user.
 * $ mysql -h <host> -u <username> -p < insert_curw_data.sql
 * You can ommit the -h <host> part if the server runs on your localhost.
 */

INSERT INTO curw.variable VALUES 
  (1, 'Precipitation'),
  (2, 'Discharge'),
  (3, 'Waterlevel'),
  (4, 'Waterdepth'),
  (5, 'Temperature');

INSERT INTO curw.type VALUES 
  (1, 'Observed'),
  (2, 'Forecast-14-d-before'),
  (3, 'Forecast-13-d-before'),
  (4, 'Forecast-12-d-before'),
  (5, 'Forecast-11-d-before'),
  (6, 'Forecast-10-d-before'),
  (7, 'Forecast-9-d-before'),
  (8, 'Forecast-8-d-before'),
  (9, 'Forecast-7-d-before'),
  (10, 'Forecast-6-d-before'),
  (11, 'Forecast-5-d-before'),
  (12, 'Forecast-4-d-before'),
  (13, 'Forecast-3-d-before'),
  (14, 'Forecast-2-d-before'),
  (15, 'Forecast-1-d-before'),
  (16, 'Forecast-0-d'),
  (17, 'Forecast-1-d-after'),
  (18, 'Forecast-2-d-after'),
  (19, 'Forecast-3-d-after'),
  (20, 'Forecast-4-d-after'),
  (21, 'Forecast-5-d-after'),
  (22, 'Forecast-6-d-after'),
  (23, 'Forecast-7-d-after'),
  (24, 'Forecast-8-d-after'),
  (25, 'Forecast-9-d-after'),
  (26, 'Forecast-10-d-after'),
  (27, 'Forecast-11-d-after'),
  (28, 'Forecast-12-d-after'),
  (29, 'Forecast-13-d-after'),
  (30, 'Forecast-14-d-after');

INSERT INTO curw.source VALUES 
  (1, 'HEC-HMS'),
  (2, 'SHER'),
  (3, 'WRF'),
  (4, 'FLO2D'),
  (5, 'EPM');

INSERT INTO curw.unit (`id`, `unit`, `type`)
VALUES
  (1, 'mm', 'Accumulative'),      # Precipitation, Evaporation
  (2, 'm3/s', 'Instantaneous'),   # Discharge
  (3, 'm', 'Instantaneous'),      # Waterdepth, Waterlevel
  (4, 'm3', 'Instantaneous'),     # Storage
  (5, 'm/s', 'Instantaneous'),    # Wind speed
  (6, 'oC', 'Instantaneous'),     # Temperature
  (7, '%', 'Instantaneous'),      # Relative humidity
  (8, 'kPa', 'Instantaneous'),    # Vapour pressure
  (9, 'Pa', 'Instantaneous'),     # Pressure
  (10, 's', 'Instantaneous');     # Time

INSERT INTO curw.station (`id`, `name`, `latitude`, `longitude`)
VALUES
  (1, 'Attanagalla', 7.111666667,  80.14983333),
  (2, 'Colombo', 6.898158, 79.8653),
  (3, 'Daraniyagala', 6.924444444, 80.33805556),
  (4, 'Glencourse', 6.978055556, 80.20305556),
  (5, 'Hanwella', 6.909722222, 80.08166667),
  (6, 'Holombuwa', 7.185166667, 80.26480556),
  (7, 'Kitulgala', 6.989166667, 80.41777778),
  (8, 'Norwood', 6.835638889, 80.61466667),
  (9, 'Kalutara', 6.6, 79.95),
  (10, 'Kalawana', 6.54, 80.38),
  (11, 'Ratnapura', 6.72, 80.38),
  (12, 'Kahawatta', 6.6, 80.58),
  (13, 'Dodampe', 6.72712, 80.3274);

# Insert Waterlevel Extraction point locations
INSERT INTO curw.station (`id`, `name`, `latitude`, `longitude`)
VALUES
    (101, 'Ambathale', 108679.790, 193253.090),
    (102, 'Madiwela-Out', 108679.790, 193003.090),
    (103, 'Salalihini-Out', 105679.790, 194003.090),
    (104, 'Salalihini-Out-2', 105679.790, 194253.090),
    (105, 'Kittampahuwa-Bridge', 103179.790, 193003.090),
    (106, 'Kittampahuwa-Out', 102429.790, 194753.090),
    (107, 'Kittampahuwa-Out-2', 102429.790, 195003.090),
    (108, 'N-Street', 100929.790, 195003.090),
    (109, 'N-Street-2', 101179.790, 195503.090),
    (110, 'Kolonnawa-CNL-1', 101179.790, 193753.090),
    (111, 'Kolonnawa-CNL-2', 102429.790, 191503.090),
    (112, 'Kolonnawa-CNL-3', 103429.790, 190753.090),
    (113, 'Kolonnawa-CNL-4', 103929.790, 189753.090),
    (114, 'Parliament-Lake-Out', 104429.790, 189253.090),
    (115, 'Parliament-Lake', 105429.790, 188003.090),
    (116, 'Parliament-Lake-2', 105429.790, 187253.090),
    (117, 'Parliament-Upstream', 106429.790, 186503.090),
    (118, 'Ahangama', 105929.790, 188003.090),
    (119, 'Madiwela-US', 110679.790, 189003.090),
    (120, 'Heen-Ela', 102179.790, 190003.090),
    (121, 'Torington', 100929.790, 189003.090),
    (122, 'Wellawatta-1', 101929.790, 187253.090),
    (123, 'Wellawatta-2', 101429.790, 187753.090),
    (124, 'Wellawatta-3', 99179.790, 186753.090),
    (125, 'Dehiwala-1', 100429.790, 186503.090),
    (126, 'Dehiwala-2', 99429.790, 185003.090);