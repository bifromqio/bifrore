package com.example;

import com.bifrore.BifroRE;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class App {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        BifroRE engine = new BifroRE("127.0.0.1", 1883, extractRuleResource());
        engine.onNext(result -> {
            System.out.println("payload=" + prettyPayload(result.payload));
            System.out.println("destinations=" + result.destinationsJson);
        });
        engine.start();

        Runtime.getRuntime().addShutdownHook(new Thread(engine::close));
        Thread.currentThread().join();
    }

    private static String prettyPayload(byte[] payload) {
        try {
            JsonNode node = MAPPER.readTree(payload);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (Exception ignored) {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    private static String extractRuleResource() throws Exception {
        try (InputStream input = App.class.getResourceAsStream("/com/example/rule.json")) {
            if (input == null) {
                throw new IllegalStateException("missing resource: /com/example/rule.json");
            }
            Path tempFile = Files.createTempFile("bifrore-rule-", ".json");
            Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            return tempFile.toAbsolutePath().toString();
        }
    }
}
