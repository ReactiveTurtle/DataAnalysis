package ru.reactiveturtle.coviddataanalysis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            String createTableQuery = "CREATE TABLE IF NOT EXISTS csse_covid_19_daily_reports " +
                    "(" +
                    "csse_covid_19_daily_reports_id INT NOT NULL AUTO_INCREMENT," +
                    "fips VARCHAR(100) NOT NULL," +
                    "admin2 VARCHAR(100) NOT NULL," +
                    "province_state VARCHAR(100) NOT NULL," +
                    "country_region VARCHAR(100) NOT NULL," +
                    "last_update DATETIME NOT NULL," +
                    "latitude DOUBLE NOT NULL," +
                    "longitude DOUBLE NOT NULL," +
                    "confirmed INT NOT NULL," +
                    "deaths INT NOT NULL," +
                    "recovered INT NOT NULL," +
                    "active INT NOT NULL," +
                    "combined_key VARCHAR(100) NOT NULL," +
                    "incident_rate DOUBLE NOT NULL," +
                    "case_fatality_ratio DOUBLE NOT NULL," +
                    "PRIMARY KEY ( csse_covid_19_daily_reports_id )" +
                    ")";
            statement.executeUpdate(createTableQuery);
            CovidDatabaseWorldFiller covidDatabaseWorldFiller = new CovidDatabaseWorldFiller(connection);
            covidDatabaseWorldFiller.parseAndFill();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            String createTableQuery = "CREATE TABLE IF NOT EXISTS csse_covid_19_daily_reports_us " +
                    "(" +
                    "csse_covid_19_daily_reports_us_id INT NOT NULL AUTO_INCREMENT," +
                    "province_state VARCHAR(100) NOT NULL," +
                    "country_region VARCHAR(100) NOT NULL," +
                    "last_update DATETIME NOT NULL," +
                    "latitude DOUBLE NOT NULL," +
                    "longitude DOUBLE NOT NULL," +
                    "confirmed INT NOT NULL," +
                    "deaths INT NOT NULL," +
                    "recovered INT NOT NULL," +
                    "active INT NOT NULL," +
                    "fips DOUBLE NOT NULL," +
                    "incident_rate DOUBLE NOT NULL," +
                    "people_tested DOUBLE NOT NULL," +
                    "people_hospitalized DOUBLE NOT NULL," +
                    "mortality_rate DOUBLE NOT NULL," +
                    "uid DOUBLE NOT NULL," +
                    "iso3 VARCHAR(100) NOT NULL," +
                    "testing_rate DOUBLE NOT NULL," +
                    "hospitalization_rate DOUBLE NOT NULL," +
                    "cases_28_days INT NOT NULL," +
                    "deaths_28_days INT NOT NULL," +
                    "PRIMARY KEY ( csse_covid_19_daily_reports_us_id )" +
                    ")";
            statement.executeUpdate(createTableQuery);
            CovidDatabaseUSFiller covidDatabaseUSFiller = new CovidDatabaseUSFiller(connection);
            covidDatabaseUSFiller.parseAndFill();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException, IOException {

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get("src/main/resources/database.properties"))) {
            props.load(in);
        }
        String url = props.getProperty("url");
        String username = props.getProperty("username");
        String password = props.getProperty("password");

        return DriverManager.getConnection(url, username, password);
    }
}
