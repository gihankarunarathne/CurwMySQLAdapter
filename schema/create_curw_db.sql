/* 
 * CFCWM database for Weather Data Store - create_cfcwm.sql.
 * You need to run this script with an authorized user.
 * $ mysql -h <host> -u <username> -p < create_mysql.sql
 * You can ommit the -h <host> part if the server runs on your localhost.
 */

CREATE DATABASE IF NOT EXISTS curw;

SELECT 'Show Databases ::';
SHOW DATABASES;                -- List the name of all the databases in this server
SELECT '\n';

/*
 * Create New User
 */
SELECT @abc := (SELECT 1  FROM mysql.user WHERE user = 'curw');
IF abc = 1 THEN
    # Create a user and grant permissions
    CREATE USER 'curw'@'localhost' IDENTIFIED BY '<PASSWORD>';
    GRANT INSERT ON curw.* TO 'curw'@'localhost';
    GRANT SELECT ON curw.* TO 'curw'@'localhost';
    GRANT UPDATE ON curw.* TO 'curw'@'localhost';
    # GRANT DELETE ON curw.* TO 'curw'@'localhost';
    # REVOKE DELETE ON curw.* FROM 'curw'@'localhost';

    # Crete User for WRF and grant permissions
    CREATE USER 'curw'@'10.138.0.3' IDENTIFIED BY '<PASSWORD>';
    GRANT INSERT ON curw.* TO 'curw'@'10.138.0.3';
    GRANT SELECT ON curw.* TO 'curw'@'10.138.0.3';
    GRANT UPDATE ON curw.* TO 'curw'@'10.138.0.3';
    # GRANT DELETE ON curw.* TO 'curw'@'10.138.0.3';
    # REVOKE DELETE ON curw.* FROM 'curw'@'10.138.0.3';

    # Crete User for Windows Instance and grant permissions
    CREATE USER 'curw'@'10.138.0.4' IDENTIFIED BY '<PASSWORD>';
    GRANT INSERT ON curw.* TO 'curw'@'10.138.0.4';
    GRANT SELECT ON curw.* TO 'curw'@'10.138.0.4';
    GRANT UPDATE ON curw.* TO 'curw'@'10.138.0.4';
    # GRANT DELETE ON curw.* TO 'curw'@'10.138.0.4';
    # REVOKE DELETE ON curw.* FROM 'curw'@'10.138.0.4';
END IF;
/*
 * Create New Database if not exists
 */
# Store CUrW chunk timeseries data. E.g. 'Forecast-0-d', 'Forecast-1-d' etc
CREATE DATABASE IF NOT EXISTS curw;
# Store CUrW continous timeseries data. E.g. 'Observed', 'Forecast'
CREATE DATABASE IF NOT EXISTS curwts;

