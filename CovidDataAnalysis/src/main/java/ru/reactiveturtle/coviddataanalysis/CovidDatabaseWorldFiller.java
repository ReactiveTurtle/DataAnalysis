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

public class CovidDatabaseWorldFiller {
    private final Connection connection;

    public CovidDatabaseWorldFiller(Connection connection) {
        Objects.requireNonNull(connection);
        this.connection = connection;
    }

    private static final String[][] POSSIBLE_COLUMN_NAMES = new String[][]{
            new String[]{"FIPS"},
            new String[]{"Admin2"},
            new String[]{"Province_State", "Province/State"},
            new String[]{"Country_Region", "Country/Region"},
            new String[]{"Last_Update", "Last Update"},
            new String[]{"Lat", "Latitude"},
            new String[]{"Long_", "Longitude"},
            new String[]{"Confirmed"},
            new String[]{"Deaths"},
            new String[]{"Recovered"},
            new String[]{"Active"},
            new String[]{"Combined_Key"},
            new String[]{"Incident_Rate", "Incidence_Rate"},
            new String[]{"Case_Fatality_Ratio", "Case-Fatality_Ratio"}
    };

    private static final String[] DATABASE_COLUMN_NAMES = new String[]{
            "fips",
            "admin2",
            "province_state",
            "country_region",
            "last_update",
            "latitude",
            "longitude",
            "confirmed",
            "deaths",
            "recovered",
            "active",
            "combined_key",
            "incident_rate",
            "case_fatality_ratio"
    };

    public void parseAndFill() throws IOException, SQLException {
        File[] csvFiles = new File(
                "D:\\Загрузки\\COVID-19-master\\COVID-19-master\\" +
                        "csse_covid_19_data\\csse_covid_19_daily_reports")
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

        StringBuilder insertQuery = new StringBuilder("INSERT csse_covid_19_daily_reports("
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
                                columnIndexInDatabase == 3 ||
                                columnIndexInDatabase == 4 ||
                                columnIndexInDatabase == 11;
                if (columnIndexInDatabase == 4) {
                    record.append("STR_TO_DATE(");
                }
                if (isNeedCommas) {
                    record.append("\"");
                }
                String csvRecordStr = csvRecord.get(j);
                if (isNeedCommas) {
                    if (columnIndexInDatabase == 4) {
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
                if (columnIndexInDatabase == 4) {
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
                    case 2:
                    case 3:
                    case 11:
                        rowForQuery.append("\"\"");
                        break;
                    case 4:
                        rowForQuery.append("STR_TO_DATE(\"0000-00-00 00:00:00\", \"%Y-%m-%d %H:%i:%s\")");
                        break;
                    case 5:
                    case 6:
                    case 7:
                    case 8:
                    case 9:
                    case 10:
                    case 12:
                    case 13:
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
