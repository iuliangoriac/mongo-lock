package com.mongodb.pessimistic.test_config;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;


public final class ConfigJson {

    private static ConfigJson INSTANCE;

    public static synchronized Optional<JSONObject> get(String... path) throws IOException {
        if (INSTANCE == null) {
            INSTANCE = new ConfigJson();
        }
        return INSTANCE.getJSONObject(path);
    }

    private final JSONObject config;

    private ConfigJson() throws IOException {
        String configJsonPath = System.getenv("AXP_CONFIG_JSON_PATH");
        String configJson = Files.readString(Paths.get(configJsonPath));
        this.config = new JSONObject(configJson);
    }

    private Optional<JSONObject> getJSONObject(String... path) {
        JSONObject jsonObj = config;
        for (String step : path) {
            if (jsonObj != null && jsonObj.has(step)) {
                Object obj = jsonObj.get(step);
                jsonObj = obj instanceof JSONObject ? (JSONObject) obj : null;
            } else {
                jsonObj = null;
            }
        }
        return jsonObj == null ? Optional.empty() : Optional.of(jsonObj);
    }

}
