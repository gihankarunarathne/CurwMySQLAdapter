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
  `latitude` DOUBLE NOT NULL,
  `longitude` DOUBLE NOT NULL,
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





