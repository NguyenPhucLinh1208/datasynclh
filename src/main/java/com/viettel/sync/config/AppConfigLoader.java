package com.viettel.sync.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(AppConfigLoader.class);
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = AppConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                logger.error("❌ Không tìm thấy file application.properties trong resources!");
                System.exit(1);
            }
            properties.load(input);
        } catch (IOException ex) {
            logger.error("❌ Lỗi khi đọc cấu hình", ex);
            System.exit(1);
        }
    }

    public static DbConfig getDbConfig() {
        return new DbConfig(
                properties.getProperty("datasource.url"),
                properties.getProperty("datasource.username"),
                properties.getProperty("datasource.password"),
                properties.getProperty("schema.source", "DATALAKE_CONFIG"),   // Default nếu thiếu
                properties.getProperty("schema.target", "DATALAKE_CONFIG_TD")     // Default nếu thiếu
        );
    }

    public static int getBatchSize() {
        return Integer.parseInt(properties.getProperty("app.batch-size", "1000"));
    }
}
