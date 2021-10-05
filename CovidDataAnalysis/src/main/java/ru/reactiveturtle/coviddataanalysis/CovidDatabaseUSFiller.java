package ru.reactiveturtle.coviddataanalysis;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class CovidDatabaseUSFiller {
    private final Connection connection;

    public CovidDatabaseUSFiller(Connection connection) {
        Objects.requireNonNull(connection);
        this.connection = connection;
    }

    private static final String[][] POSSIBLE_COLUMN_NAMES = new String[][]{
            new String[]{"Province_State"},
            new String[]{"Country_Region"},
            new String[]{"Last_Update"},
            new String[]{"Lat"},
            new String[]{"Long_"},
            new String[]{"Confirmed"},
            new String[]{"Deaths"},
            new String[]{"Recovered"},
            new String[]{"Active"},
            new String[]{"FIPS"},
            new String[]{"Incident_Rate"},
            new String[]{"People_Tested", "Total_Test_Results"},
            new String[]{"People_Hospitalized"},
            new String[]{"Mortality_Rate", "Case_Fatality_Ratio"},
            new String[]{"UID"},
            new String[]{"ISO3"},
            new String[]{"Testing_Rate"},
            new String[]{"Hospitalization_Rate"},
            new String[]{"Cases_28_Days"},
            new String[]{"Deaths_28_Days"}
    };

    private static final String[] DATABASE_COLUMN_NAMES = new String[]{
            "province_state",
            "country_region",
            "last_update",
            "latitude",
            "longitude",
            "confirmed",
            "deaths",
            "recovered",
            "active",
            "fips",
            "incident_rate",
            "people_tested",
            "people_hospitalized",
            "mortality_rate",
            "uid",
            "iso3",
            "testing_rate",
            "hospitalization_rate",
            "cases_28_days",
            "deaths_28_days"
    };

    public void parseAndFill() throws IOException, SQLException {
        File[] csvFiles = new File(
                "D:\\Загрузки\\COVID-19-master\\COVID-19-master\\" +
                        "csse_covid_19_data\\csse_covid_19_daily_reports_us")
                .listFiles();
        Objects.requireNonNull(csvFiles);
        for (File csvFile : csvFiles) {
            parseCSV(csvFile);
        }
    }

    private void parseCSV(File csvFile) throws IOException, SQLException {
        CSVParser parser = CSVParser.parse(
                csvFile,
                Charset.defaultCharset(),
                CSVFormat.DEFAULT.withHeader());
        List<String> headers = parser.getHeaderNames();
        System.out.println(csvFile);
        List<Integer> headerIndexes = getHeaderIndexesInDatabase(headers);
        List<CSVRecord> rows = parser.getRecords();

        StringBuilder insertQuery = new StringBuilder("INSERT csse_covid_19_daily_reports_us("
                + String.join(", ", DATABASE_COLUMN_NAMES) + ") VALUES");

        for (int i = 0; i < rows.size(); i++) {
            CSVRecord csvRecord = rows.get(i);
            List<String> formattedRowData = new ArrayList<>();
            for (int j = 0; j < csvRecord.size(); j++) {
                StringBuilder record = new StringBuilder();
                int columnIndexInDatabase = headerIndexes.get(j);
                boolean isNeedCommas =
                        columnIndexInDatabase == 0 ||
                                columnIndexInDatabase == 1 ||
                                columnIndexInDatabase == 2 ||
                                columnIndexInDatabase == 15;
                if (columnIndexInDatabase == 2) {
                    record.append("STR_TO_DATE(");
                }
                if (isNeedCommas) {
                    record.append("\"");
                }
                String csvRecordStr = csvRecord.get(j);
                if (isNeedCommas) {
                    if (columnIndexInDatabase == 2) {
                        if (csvRecordStr.isEmpty()) {
                            csvRecordStr = csvFile.getName();
                        }
                        csvRecordStr = formatDate(csvRecordStr);
                    }
                } else {
                    if (csvRecordStr == null || csvRecordStr.isEmpty() || csvRecordStr.equals("#DIV/0!")) {
                        csvRecordStr = "0";
                    }
                }
                record.append(csvRecordStr);
                if (isNeedCommas) {
                    record.append("\"");
                }
                if (columnIndexInDatabase == 2) {
                    record.append(", \"%Y-%m-%d %H:%i:%s\")");
                }
                formattedRowData.add(record.toString());
            }
            String rowForQuery = buildRowForQuery(headerIndexes, formattedRowData);
            insertQuery.append("\n").append(rowForQuery);
            if (i < rows.size() - 1) {
                insertQuery.append(",");
            }
        }
        parser.close();
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(insertQuery.toString());
        } catch (SQLException e) {
            System.out.println(insertQuery);
            throw new SQLException(e);
        }
    }

    private List<Integer> getHeaderIndexesInDatabase(List<String> headers) {
        List<Integer> indexes = new ArrayList<>();
        for (String headerName : headers) {
            int headerIndexInDatabase = -1;
            for (int i = 0; i < POSSIBLE_COLUMN_NAMES.length && headerIndexInDatabase == -1; i++) {
                for (int j = 0; j < POSSIBLE_COLUMN_NAMES[i].length; j++) {
                    if (POSSIBLE_COLUMN_NAMES[i][j].equals(headerName)) {
                        headerIndexInDatabase = i;
                        break;
                    }
                }
            }
            if (headerIndexInDatabase == -1) {
                throw new RuntimeException("Header name " + headerName + " is not declared.");
            }
            indexes.add(headerIndexInDatabase);
        }
        return indexes;
    }

    private static String formatDate(String dateStr) {
        String formattedDate = null;
        SimpleDateFormat correctDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = correctDateFormat.parse(dateStr);
            formattedDate = correctDateFormat.format(date);
        } catch (ParseException ignore) {
        }
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            Date date = simpleDateFormat.parse(dateStr);
            formattedDate = correctDateFormat.format(date);
        } catch (ParseException ignore) {
        }
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            Date date = simpleDateFormat.parse(dateStr);
            formattedDate = correctDateFormat.format(date);
        } catch (ParseException ignore) {
        }
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Date date = simpleDateFormat.parse(dateStr);
            formattedDate = correctDateFormat.format(date);
        } catch (ParseException ignore) {
        }
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM-dd-yyyy'.csv'");
            Date date = simpleDateFormat.parse(dateStr);
            formattedDate = correctDateFormat.format(date);
        } catch (ParseException ignore) {
        }
        if (formattedDate == null) {
            throw new RuntimeException("Unhandled date " + dateStr);
        }
        return formattedDate;
    }

    private String buildRowForQuery(
            List<Integer> headerIndexes,
            List<String> formattedRowData) {
        StringBuilder rowForQuery = new StringBuilder("(");
        for (int j = 0; j < DATABASE_COLUMN_NAMES.length; j++) {
            int index = headerIndexes.indexOf(j);
            if (index > -1) {
                rowForQuery.append(formattedRowData.get(index));
            } else {
                switch (j) {
                    case 0:
                    case 1:
                    case 15:
                        rowForQuery.append("\"\"");
                        break;
                    case 2:
                        throw new RuntimeException();
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                    case 9:
                    case 10:
                    case 11:
                    case 12:
                    case 13:
                    case 14:
                    case 16:
                    case 17:
                    case 18:
                    case 19:
                        rowForQuery.append("0");
                        break;
                    default:
                        throw new RuntimeException("Unhandled column at index " + j);
                }
            }
            if (j < DATABASE_COLUMN_NAMES.length - 1) {
                rowForQuery.append(", ");
            }
        }
        rowForQuery.append(")");
        return rowForQuery.toString();
    }
}
