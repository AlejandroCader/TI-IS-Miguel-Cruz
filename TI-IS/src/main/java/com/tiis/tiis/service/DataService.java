package com.tiis.tiis.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class DataService {


    @Value("${api.url}")
    private String url;

    @Value("${database.url}")
    private String dbUrl;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Scheduled(cron = "0 0 0 * * ?")
    public String fetchData() throws IOException {
        RestTemplate restTemplate = new RestTemplate();
        String response = restTemplate.getForObject(url, String.class);
        String date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Files.write(Paths.get("data_" + date + ".json"), response.getBytes());
        return date;
    }

    public void jsonToCsv(String date) throws IOException{

        String jsonPath = "data_" + date + ".json";
        String csvPath = "ETL_" + date + ".csv";

        JsonNode jsonNode = objectMapper.readTree(new File(jsonPath));
        JsonNode userNode = jsonNode.get("users");

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvPath))) {
            writer.writeNext(new String[]{"id", "firstName", "lastName", "age", "email"});
            if (userNode.isArray()) {
                for (JsonNode node : userNode) {
                    String id = node.get("id").asText();
                    String firstName = node.get("firstName").asText();
                    String lastName = node.get("lastName").asText();
                    String age = node.get("age").asText();
                    String email = node.get("email").asText();
                    writer.writeNext(new String[]{id, firstName, lastName, age, email});
                }
            }
        }
    }

    public void generateSummary(String date) throws IOException{

        String jsonPath = "data_" + date + ".json";
        String summaryPath = "summary_" + date + ".csv";

        JsonNode jsonNode = objectMapper.readTree(new File(jsonPath));
        JsonNode userNode = jsonNode.get("users");

        int totalUsers = 0;
        Map<String, Integer> genderCount = new HashMap<>();
        Map<String, int[]> ageGenderCount = new HashMap<>();
        Map<String, int[]> cityGenderCount = new HashMap<>();
        Map<String, Integer> osCount = new HashMap<>();

        String[] ageGroups = {"00-10", "11-20", "21-30", "31-40", "41-50", "51-60", "61-70", "71-80", "81-90", "91+"};
        for (String ageGroup : ageGroups) {
            ageGenderCount.put(ageGroup, new int[3]); // [male, female, other]
        }

        if (userNode.isArray()) {
            for (JsonNode node : userNode) {
                totalUsers++;

                String gender = node.get("gender").asText();

                genderCount.put(gender, genderCount.getOrDefault(gender, 0) + 1);

                int age = node.get("age").asInt();

                String ageGroup = getAgeGroup(age);

                int[] ageGender = ageGenderCount.get(ageGroup);

                if (gender.equals("male")) {
                    ageGender[0]++;
                } else if (gender.equals("female")) {
                    ageGender[1]++;
                } else {
                    ageGender[2]++;
                }

                String city = node.path("address").path("city").asText();

                cityGenderCount.putIfAbsent(city, new int[3]);

                int[] cityGender = cityGenderCount.get(city);

                if (gender.equals("male")) {
                    cityGender[0]++;
                } else if (gender.equals("female")) {
                    cityGender[1]++;
                } else {
                    cityGender[2]++;
                }
            }

        }

        try (CSVWriter writer = new CSVWriter(new FileWriter(summaryPath))) {

            writer.writeNext(new String[]{"register", String.valueOf(totalUsers)});

            writer.writeNext(new String[]{"gender", "total"});

            for (Map.Entry<String, Integer> entry : genderCount.entrySet()) {
                writer.writeNext(new String[]{entry.getKey(), String.valueOf(entry.getValue())});
            }

            writer.writeNext(new String[]{""});

            writer.writeNext(new String[]{"age", "male", "female", "other"});

            for (String ageGroup : ageGroups) {
                int[] counts = ageGenderCount.get(ageGroup);
                writer.writeNext(new String[]{ageGroup, String.valueOf(counts[0]), String.valueOf(counts[1]), String.valueOf(counts[2])});
            }

            writer.writeNext(new String[]{""});

            writer.writeNext(new String[]{"City", "male", "female", "other"});

            for (Map.Entry<String, int[]> entry : cityGenderCount.entrySet()) {
                int[] counts = entry.getValue();
                writer.writeNext(new String[]{entry.getKey(), String.valueOf(counts[0]), String.valueOf(counts[1]), String.valueOf(counts[2])});
            }
        }
    }

    private String getAgeGroup(int age) {
        if (age <= 10) return "00-10";
        else if (age <= 20) return "11-20";
        else if (age <= 30) return "21-30";
        else if (age <= 40) return "31-40";
        else if (age <= 50) return "41-50";
        else if (age <= 60) return "51-60";
        else if (age <= 70) return "61-70";
        else if (age <= 80) return "71-80";
        else if (age <= 90) return "81-90";
        else return "91+";
    }

    public void saveToDatabase(String date) {
        String summaryPath = "summary_" + date + ".csv";
        String detailPath = "ETL_" + date + ".csv";
        String dbFilePath = dbUrl;

        try (Connection conn = connect(dbFilePath)) {
            createTables(conn);

            long processId = insertProcess(conn, date);

            insertCsvData(conn, summaryPath, "summary_table", processId);
            insertCsvData(conn, detailPath, "detail_table", processId);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private Connection connect(String dbFilePath) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFilePath);
    }

    private void createTables(Connection conn) throws SQLException {
        String createProcessTable = "CREATE TABLE IF NOT EXISTS process_table ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "execution_date TEXT NOT NULL"
                + ");";

        String createSummaryTable = "CREATE TABLE IF NOT EXISTS summary_table ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "process_id INTEGER,"
                + "register INTEGER,"
                + "gender TEXT,"
                + "total INTEGER,"
                + "FOREIGN KEY (process_id) REFERENCES process_table(id)"
                + ");";

        String createDetailTable = "CREATE TABLE IF NOT EXISTS detail_table ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                + "process_id INTEGER,"
                + "user_id INTEGER,"
                + "first_name TEXT,"
                + "last_name TEXT,"
                + "age INTEGER,"
                + "email TEXT,"
                + "FOREIGN KEY (process_id) REFERENCES process_table(id)"
                + ");";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createProcessTable);
            stmt.execute(createSummaryTable);
            stmt.execute(createDetailTable);
        }
    }

    private long insertProcess(Connection conn, String date) throws SQLException {
        String sql = "INSERT INTO process_table(execution_date) VALUES(?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, date);
            pstmt.executeUpdate();
            try (var rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to insert process");
    }

    private void insertCsvData(Connection conn, String csvPath, String tableName, long processId) throws IOException, SQLException {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(csvPath)).withSkipLines(1).build()) {
            String insertSql = tableName.equals("summary_table")
                    ? "INSERT INTO summary_table(process_id, register, gender, total) VALUES(?, ?, ?, ?)"
                    : "INSERT INTO detail_table(process_id, user_id, first_name, last_name, age, email) VALUES(?, ?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                String[] line;
                while ((line = reader.readNext()) != null) {
                    pstmt.setLong(1, processId);
                    for (int i = 0; i < line.length; i++) {
                        pstmt.setString(i + 2, line[i]);
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
    }
}
