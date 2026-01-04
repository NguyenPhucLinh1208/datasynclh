package com.viettel.sync.repository;

import com.viettel.sync.config.AppConfigLoader;
import com.viettel.sync.core.GenericRepository;
import com.viettel.sync.model.target.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TargetRepository {
    private final String targetSchema;
    public final GenericRepository<TargetConnection> connectionRepo = new GenericRepository<>();
    public final GenericRepository<TargetCommand> commandRepo = new GenericRepository<>();
    public final GenericRepository<TargetConfig> configRepo = new GenericRepository<>();
    public final GenericRepository<TargetCreate> createRepo = new GenericRepository<>();
    public final GenericRepository<TargetClean> cleanRepo = new GenericRepository<>();
    public final GenericRepository<TargetTimeParam> timeRepo = new GenericRepository<>();

    public TargetRepository() { this.targetSchema = AppConfigLoader.getDbConfig().targetSchema(); }
    public String getTargetSchema() { return targetSchema; }

    // Helper
    private Long getLong(ResultSet rs, String col) { try { return rs.getLong(col); } catch(Exception e) { return 0L; } }
    private Timestamp getTime(ResultSet rs, String col) { try { return rs.getTimestamp(col); } catch(Exception e) { return null; } }

    public List<TargetConnection> getAllTargetConnections(Connection conn) throws Exception {
        List<TargetConnection> list = new ArrayList<>();
        String sql = "SELECT * FROM " + targetSchema + ".D_CONNECTION";
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetConnection(
                        rs.getLong("ID"), rs.getString("URL"), rs.getString("USER_NAME"), rs.getString("PASS"),
                        rs.getString("DESCRIPTION"), rs.getString("CONNECTION_NAME"), rs.getString("DRIVER_NAME"),
                        rs.getLong("ID_DB"), rs.getString("PORT"), rs.getString("TYPE_DB"), getTime(rs, "INSERT_DATE")
                ));
            }
        }
        return list;
    }

    public List<TargetCommand> getAllTargetCommands(Connection conn) throws Exception {
        List<TargetCommand> list = new ArrayList<>();
        String sql = "SELECT * FROM " + targetSchema + ".D_DB_2_HDFS_COMMAND";
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetCommand(
                        rs.getLong("ID"), rs.getLong("ID_CONNECTION"),
                        getLong(rs, "FETCH_SIZE"), getLong(rs, "USE_PARTITION"), getLong(rs, "NUM_FIELDS"),
                        getLong(rs, "NUM_EXES"), getLong(rs, "NUM_PARTS"), rs.getString("DESCRIPTION"),
                        getLong(rs, "USE_SUBPARTITION"), rs.getString("PARAMS"), getLong(rs, "ID_DB"),
                        rs.getString("SPLIT_COLUMN"), rs.getString("MASK_COLUMN"),
                        getTime(rs, "INSERT_DATE"), rs.getString("SQL_COMMAND")
                ));
            }
        }
        return list;
    }

    public List<TargetConfig> getAllTargetConfigs(Connection conn) throws Exception {
        List<TargetConfig> list = new ArrayList<>();
        String sql = "SELECT * FROM " + targetSchema + ".D_DB_2_HDFS_CONFIG";
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetConfig(
                        rs.getLong("ID"), rs.getLong("ID_COMMAND"),
                        rs.getObject("ID_COMMAND_CREATE") != null ? rs.getLong("ID_COMMAND_CREATE") : null,
                        rs.getString("TABLE_NAME"), rs.getString("LOCATION_PATH"), rs.getString("REMOVE_PATH"),
                        rs.getString("SOURCE"), rs.getString("DESCRIPTION"), getLong(rs, "IS_ACTIVE"),
                        rs.getObject("ID_GROUP") != null ? rs.getLong("ID_GROUP") : null,
                        getLong(rs, "MAX_TIME"), getTime(rs, "INSERT_DATE"),
                        rs.getString("OUTPUT_FORMAT"), rs.getString("IMPORT_TYPE")
                ));
            }
        }
        return list;
    }

    public List<TargetCreate> getAllTargetCreates(Connection conn) throws Exception {
        List<TargetCreate> list = new ArrayList<>();
        String sql = "SELECT * FROM " + targetSchema + ".D_DB_2_HDFS_COMMAND_CREATE";
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetCreate(
                        rs.getLong("ID"),
                        getLong(rs, "ID_CONNECTION"),
                        rs.getString("DESCRIPTION"),
                        getTime(rs, "INSERT_DATE"),
                        rs.getString("SQL_COMMAND")
                ));
            }
        }
        return list;
    }

    public List<TargetTimeParam> getAllTargetTimeParams(Connection conn) throws Exception {
        List<TargetTimeParam> list = new ArrayList<>();
        String sql = "SELECT * FROM " + targetSchema + ".D_TIME_PARAM_CONFIG";
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetTimeParam(
                        rs.getString("NAME"), rs.getInt("ADD_DAY"), rs.getInt("ADD_MON"), rs.getInt("ADD_YEAR"),
                        rs.getString("FORMAT"), rs.getString("EXTEND_FORMAT"), rs.getInt("ADD_MIN"), rs.getInt("ADD_HOUR")
                ));
            }
        }
        return list;
    }
}
