package com.viettel.sync.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseUtils.class);

    private DatabaseUtils() {}

    public static Connection getConnection() throws SQLException {
        DbConfig config = AppConfigLoader.getDbConfig();
        logger.debug("Opening connection to DB: {}", config.url());
        Connection conn = DriverManager.getConnection(config.url(), config.username(), config.password());
        conn.setAutoCommit(false);
        return conn;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error closing connection", e);
            }
        }
    }
}
