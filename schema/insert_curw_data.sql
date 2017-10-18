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
  (2, 'Forecast'), # Store CUrW continous timeseries data. E.g. 'Observed', 'Forecast'
  (3, 'Forecast-14-d-before'), # Store CUrW chunk timeseries data. E.g. 'Forecast-0-d', 'Forecast-1-d' etc
  (4, 'Forecast-13-d-before'),
  (5, 'Forecast-12-d-before'),
  (6, 'Forecast-11-d-before'),
  (7, 'Forecast-10-d-before'),
  (8, 'Forecast-9-d-before'),
  (9, 'Forecast-8-d-before'),
  (10, 'Forecast-7-d-before'),
  (11, 'Forecast-6-d-before'),
  (12, 'Forecast-5-d-before'),
  (13, 'Forecast-4-d-before'),
  (14, 'Forecast-3-d-before'),
  (15, 'Forecast-2-d-before'),
  (16, 'Forecast-1-d-before'),
  (17, 'Forecast-0-d'),
  (18, 'Forecast-1-d-after'),
  (19, 'Forecast-2-d-after'),
  (20, 'Forecast-3-d-after'),
  (21, 'Forecast-4-d-after'),
  (22, 'Forecast-5-d-after'),
  (23, 'Forecast-6-d-after'),
  (24, 'Forecast-7-d-after'),
  (25, 'Forecast-8-d-after'),
  (26, 'Forecast-9-d-after'),
  (27, 'Forecast-10-d-after'),
  (28, 'Forecast-11-d-after'),
  (29, 'Forecast-12-d-after'),
  (30, 'Forecast-13-d-after'),
  (31, 'Forecast-14-d-after');

INSERT INTO curw.source VALUES 
  (1, 'HEC-HMS', NULL),
  (2, 'SHER', NULL),
  (3, 'WRF', NULL),
  (4, 'FLO2D', NULL),
  (5, 'EPM', NULL),
  (6, 'WeatherStation', NULL),
  (7, 'WaterLevelGuage', NULL);

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

INSERT INTO curw.station (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`)
VALUES
  (100001, 'curw_attanagalla', 'Attanagalla', 7.111666667,  80.14983333, 0),
  (100002, 'curw_colombo', 'Colombo', 6.898158, 79.8653, 0),
  (100003, 'curw_daraniyagala', 'Daraniyagala', 6.924444444, 80.33805556, 0),
  (100004, 'curw_glencourse', 'Glencourse', 6.978055556, 80.20305556, 0),
  (100005, 'curw_hanwella', 'Hanwella', 6.909722222, 80.08166667, 0),
  (100006, 'curw_holombuwa', 'Holombuwa', 7.185166667, 80.26480556, 0),
  (100007, 'curw_kitulgala', 'Kitulgala', 6.989166667, 80.41777778, 0),
  (100008, 'curw_norwood', 'Norwood', 6.835638889, 80.61466667, 0),
  (100009, 'curw_kalutara', 'Kalutara', 6.6, 79.95, 0),
  (100010, 'curw_kalawana', 'Kalawana', 6.54, 80.38, 0),
  (100011, 'curw_ratnapura', 'Ratnapura', 6.72, 80.38, 0),
  (100012, 'curw_kahawatta', 'Kahawatta', 6.6, 80.58, 0),
  (100013, 'curw_dodampe', 'Dodampe', 6.72712, 80.3274, 0);

# Insert Waterlevel Extraction point locations
INSERT INTO curw.station (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`)
VALUES
  (1200001, "sim_flo2d_n'street_river", "N'Street-River", 79.877339451000068, 6.959254587000032, 0),
  (1200002, "sim_flo2d_n'street_canal", "N'Street-Canal", 79.877348052000059, 6.954733548000036, 0),
  (1200003, 'sim_flo2d_wellawatta', 'Wellawatta', 79.861655902000052, 6.880106281000053, 0),
  (1200004, 'sim_flo2d_dematagoda_canal', 'Dematagoda-Canal', 79.879631729000039, 6.943435232000070, 0),
  (1200005, 'sim_flo2d_dehiwala', 'Dehiwala', 79.863636663000079, 6.863283492000051, 0),
  (1200006, 'sim_flo2d_parliament_lake_bridge_kotte_canal', 'Parliament Lake Bridge-Kotte Canal', 79.902332838000063, 6.900527256000032, 0),
  (1200007, 'sim_flo2d_parliament_lake_out', 'Parliament Lake-Out', 79.915921237000077, 6.891509735000057, 0),
  (1200008, 'sim_flo2d_madiwela_us', 'Madiwela-US', 79.926958523000053, 7.042986704000043, 0),
  (1200009, 'sim_flo2d_ambathale', 'Ambathale', 79.947506252000039, 6.939037595000059, 0),
  (1200010, 'sim_flo2d_madiwela_out', 'Madiwela-Out', 79.947510206000061, 6.936777033000055, 0),
  (1200011, 'sim_flo2d_salalihini_river', 'Salalihini-River', 79.918081126000061, 6.948027491000062, 0),
  (1200012, 'sim_flo2d_salalihini_canal', 'Salalihini-Canal', 79.920347460000073, 6.945771037000043, 0),
  (1200013, 'sim_flo2d_kittampahuwa_river', 'Kittampahuwa-River', 79.890921590000062, 6.954759129000024, 0),
  (1200014, 'sim_flo2d_kittampahuwa_out', 'Kittampahuwa-Out', 79.890925824000078, 6.952498601000059, 0),
  (1200015, 'sim_flo2d_kolonnawa_canal', 'Kolonnawa-Canal', 79.890980733000049, 6.923111719000076, 0),
  (1200016, 'sim_flo2d_heen_ela', 'Heen Ela', 79.884245272000044, 6.895972700000073, 0),
  (1200017, 'sim_flo2d_torington', 'Torington', 79.877450821000082, 6.900481020000029, 0),
  (1200018, 'sim_flo2d_parliament_lake', 'Parliament Lake', 79.918183212000031, 6.891513804000056, 0);