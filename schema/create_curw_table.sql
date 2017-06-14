CREATE TABLE `curw`.`variable` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `variable` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `curw`.`type` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `type_UNIQUE` (`type` ASC)
);

CREATE TABLE `curw`.`unit` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `unit` VARCHAR(10) NOT NULL,
  `type` ENUM('Accumulative', 'Instantaneous') NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `curw`.`source` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `source` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `source_UNIQUE` (`source` ASC)
);

CREATE TABLE `curw`.`station` (
  `id` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `latitude` FLOAT NOT NULL,
  `longitude` FLOAT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC)
);

CREATE TABLE `curw`.`run` (
  `id` VARCHAR(64) NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `start_date` DATETIME NOT NULL,
  `end_date` DATETIME NOT NULL,
  `station` INT NOT NULL,
  `variable` INT NOT NULL,
  `unit` INT NOT NULL,
  `type` INT NOT NULL,
  `source` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC),
  INDEX `station_idx` (`station` ASC),
  INDEX `variable_idx` (`variable` ASC),
  INDEX `unit_idx` (`unit` ASC),
  INDEX `type_idx` (`type` ASC),
  INDEX `source_idx` (`source` ASC),
  CONSTRAINT `station`
    FOREIGN KEY (`station`)
    REFERENCES `curw`.`station` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `variable`
    FOREIGN KEY (`variable`)
    REFERENCES `curw`.`variable` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `unit`
    FOREIGN KEY (`unit`)
    REFERENCES `curw`.`unit` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `type`
    FOREIGN KEY (`type`)
    REFERENCES `curw`.`type` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE,
  CONSTRAINT `source`
    FOREIGN KEY (`source`)
    REFERENCES `curw`.`source` (`id`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE
);

CREATE TABLE `curw`.`data` (
  `id` VARCHAR(64) NOT NULL,
  `time` DATETIME NOT NULL,
  `value` DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (`id`, `time`),
  CONSTRAINT `id`
    FOREIGN KEY (`id`)
    REFERENCES `curw`.`run` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

use curw;
CREATE VIEW `run_view` AS
  SELECT 
    `run`.`id` AS `id`,
    `run`.`name` AS `name`,
    `run`.`start_date` AS `start_date`,
    `run`.`end_date` AS `end_date`,
    `station`.`name` AS `station`,
    `variable`.`variable` AS `variable`,
    `unit`.`unit` AS `unit`,
    `type`.`type` AS `type`,
    `source`.`source` AS `source`
  FROM
    (((((`run`
    JOIN `station` ON ((`run`.`station` = `station`.`id`)))
    JOIN `variable` ON ((`run`.`variable` = `variable`.`id`)))
    JOIN `unit` ON ((`run`.`unit` = `unit`.`id`)))
    JOIN `type` ON ((`run`.`type` = `type`.`id`)))
    JOIN `source` ON ((`run`.`source` = `source`.`id`)));

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





