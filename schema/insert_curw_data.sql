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
  (5, 'Temperature'),
  (6, 'Humidity'),
  (7, 'WindSpeed'),
  (8, 'WindDirection'),
  (9, 'WindGust'),
  (10, 'SolarRadiation');

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
  (1, '-', 'Instantaneous'),      # Dimentionless
  (2, 'mm', 'Accumulative'),      # Precipitation, Evaporation
  (3, 'm3/s', 'Instantaneous'),   # Discharge
  (4, 'm', 'Instantaneous'),      # Waterdepth, Waterlevel
  (5, 'm3', 'Instantaneous'),     # Storage
  (6, 'm/s', 'Instantaneous'),    # Wind speed
  (7, 'oC', 'Instantaneous'),     # Temperature
  (8, '%', 'Instantaneous'),      # Relative humidity
  (9, 'kPa', 'Instantaneous'),    # Vapour pressure
  (10, 'Pa', 'Instantaneous'),    # Pressure
  (11, 's', 'Instantaneous'),     # Time
  (12, 'degrees', 'Instantaneous'),# Wind Direction
  (13, 'W/m2', 'Instantaneous');  # Solar Radiation

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
  (1200001, 'sim_flo2d_nagalagam_street_river', 'Nagalagam Street River', 6.959254587, 79.877339451, 0),
  (1200002, 'sim_flo2d_nagalagam_street', 'Nagalagam Street', 6.9569444444, 79.8780194444, 0),
  (1200003, 'sim_flo2d_wellawatta_canal_st_peters_college', 'Wellawatta Canal-St Peters College', 6.88055, 79.8621972222, 0),
  (1200004, 'sim_flo2d_dematagoda_canal_orugodawatta', 'Dematagoda Canal-Orugodawatta', 6.9434861111, 79.8789361111, 0),
  (1200005, 'sim_flo2d_dehiwala', 'Dehiwala', 6.863283492, 79.863636663, 0),
  (1200006, 'sim_flo2d_parliament_lake_bridge_kotte_canal', 'Parliament Lake Bridge-Kotte Canal', 6.900527256, 79.902332838, 0),
  (1200007, 'sim_flo2d_parliament_lake_out', 'Parliament Lake-Out', 6.891509735, 79.915921237, 0),
  (1200008, 'sim_flo2d_madiwela_us', 'Madiwela-US', 7.042986704, 79.926958523, 0),
  (1200009, 'sim_flo2d_ambathale_river', 'Ambatale River', 6.939037595, 79.947506252, 0),
  (1200010, 'sim_flo2d_ambatale_outfull1', 'Ambatale Outfull1', 6.9375222222, 79.9465416667, 0),
  (1200011, 'sim_flo2d_salalihini_river', 'Salalihini-River', 6.948027491, 79.918081126, 0),
  (1200012, 'sim_flo2d_salalihini_canal', 'Salalihini-Canal', 6.945771037, 79.92034746, 0),
  (1200013, 'sim_flo2d_kittampahuwa_river', 'Kittampahuwa River', 79.89092159, 6.954759129, 0),
  (1200014, 'sim_flo2d_kalupalama', 'Kalupalama', 6.9536805556, 79.8899472222, 0),
  (1200015, 'sim_flo2d_yakbedda', 'Yakbedda', 6.9237055556, 79.8912, 0),
  (1200016, 'sim_flo2d_heen_ela', 'Heen Ela', 6.8959727, 79.884245272, 0),
  (1200017, 'sim_flo2d_torrinton', 'Torrinton', 6.8995166667, 79.877325, 0),
  (1200018, 'sim_flo2d_parliament_lake', 'Parliament Lake', 6.891513804, 79.918183212, 0),
  (1200019, 'sim_flo2d_kotte_north_canal', 'Kotte North Canal', 6.9063944444, 79.9034166667, 0),
  (1200020, 'sim_flo2d_ousl_nawala_kirulapana_canal', 'OUSL-Nawala Kirulapana Canal', 6.8837, 79.8867194444, 0),
  (1200021, 'sim_flo2d_dehiwala_canal', 'Dehiwala Canal', 79.8635111111, 6.8631888889, 0),
  (1200022, 'sim_flo2d_near_sllrdc', 'Near SLLRDC', 79.8890111111, 6.9097555556, 0),
  (1200023, 'sim_flo2d_kirimandala_mw', 'Kirimandala Mw', 79.8840222222, 6.8945555556, 0),
  (1200024, 'sim_flo2d_ousl_narahenpita_rd', 'OUSL-Narahenpita Rd', 79.8807583333, 6.888825, 0),
  (1200025, 'sim_flo2d_swarna_rd_wellawatta', 'Swarna Rd-Wellawatta', 79.8711861111, 6.8832, 0),
  (1200026, 'sim_flo2d_mutwal_outfall', 'Mutwal Outfall', 79.8578027778, 6.9620416667, 0),
  (1200027, 'sim_flo2d_thummodara', 'Thummodara', 79.8710888889, 6.8850583333, 0),
  (1200028, 'sim_flo2d_janakalakendraya', 'JanakalaKendraya', 79.9217277778, 6.8919777778, 0),
  (1200029, 'sim_flo2d_kotiyagoda', 'Kotiyagoda', 79.9042444444, 6.9102222222, 0),
  (1200030, 'sim_flo2d_lesliranagala_mw', 'LesliRanagala Mw', 79.8841416667, 6.9175944444, 0),
  (1200031, 'sim_flo2d_babapulle', 'Babapulle', 79.87205, 6.9420222222, 0),
  (1200032, 'sim_flo2d_ingurukade_jn', 'Ingurukade Jn', 79.8746916667, 6.9510083333, 0),
  (1200033, 'sim_flo2d_amaragoda', 'Amaragoda', 79.9576166667, 6.8886666667, 0),
  (1200034, 'sim_flo2d_malabe', 'Malabe', 79.9633138889, 6.9059305556, 0),
  (1200035, 'sim_flo2d_madinnagoda', 'Madinnagoda', 79.8993472222, 6.9160277778, 0),
  (1200036, 'sim_flo2d_kittampahuwa', 'Kittampahuwa', 79.8859611111, 6.94525, 0),
  (1200037, 'sim_flo2d_weliwala_pond', 'Weliwala Pond', 79.9167055556, 6.9278916667, 0),
  (1200038, 'sim_flo2d_old_awissawella_rd', 'Old Awissawella Rd', 79.9244888889, 6.9431666667, 0),
  (1200039, 'sim_flo2d_kelani_mulla_outfall', 'Kelani Mulla Outfall', 79.9194222222, 6.9467722222, 0),
  (1200040, 'sim_flo2d_wellampitiya', 'Wellampitiya', 79.8972388889, 6.9371805556, 0),
  (1200041, 'sim_flo2d_talatel_culvert', 'Talatel Culvert', 79.9266416667, 6.9325916667, 0),
  (1200042, 'sim_flo2d_wennawatta', 'Wennawatta', 79.9281611111, 6.9359694444, 0),
  (1200043, 'sim_flo2d_vivekarama_mw', 'Vivekarama Mw', 79.8971583333, 6.9405805556, 0),
  (1200044, 'sim_flo2d_koratuwa_rd', 'Koratuwa Rd', 79.9125972222, 6.9309638889, 0),
  (1200045, 'sim_flo2d_harwad_band', 'Harwad Band', 79.9023333333, 6.9292194444, 0),
  (1200046, 'sim_flo2d_baira_lake_nawam_mw', 'Baira Lake Nawam Mw', 79.8611055556, 6.9207638889, 0),
  (1200047, 'sim_flo2d_baira_lake_railway', 'Baira Lake Railway', 79.8479527778, 6.9339333333, 0),
  (1200048, 'sim_flo2d_kibulawala_1', 'Kibulawala 1', 79.9316305556, 6.8667305556, 0),
  (1200049, 'sim_flo2d_kibulawala_2', 'Kibulawala 2', 79.9308972222, 6.8668222222, 0),
  (1200050, 'sim_flo2d_parlimant_lake_side', 'Parlimant Lake Side', 79.9277666667, 6.877225, 0),
  (1200051, 'sim_flo2d_polduwa_parlimant_rd', 'Polduwa-Parlimant Rd', 79.9090222222, 6.9035083333, 0),
  (1200052, 'sim_flo2d_abagaha_jn', 'Abagaha Jn', 79.9085861111, 6.9128944444, 0),
  (1200053, 'sim_flo2d_aggona', 'Aggona', 79.9267666667, 6.9158916667, 0),
  (1200054, 'sim_flo2d_rampalawatta', 'Rampalawatta', 79.9328, 6.8841527778, 0);
