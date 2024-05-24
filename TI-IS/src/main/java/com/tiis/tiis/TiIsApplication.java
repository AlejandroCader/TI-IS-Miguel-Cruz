package com.tiis.tiis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVWriter;
import com.tiis.tiis.service.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class TiIsApplication {
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) {
		//SpringApplication.run(TiIsApplication.class, args);

		try {
			String date = "20240523";

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

		} catch (IOException e){
			return;
		}

	}

	public static String getAgeGroup(int age) {
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

}
