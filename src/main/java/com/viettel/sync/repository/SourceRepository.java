package com.viettel.sync.repository;

import com.viettel.sync.config.AppConfigLoader;
import com.viettel.sync.model.source.SourceConfig;
import com.viettel.sync.model.source.UnifiedSourceDTO;
import com.viettel.sync.model.target.TargetConnection;
import com.viettel.sync.model.target.TargetTimeParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SourceRepository {
    private static final Logger logger = LoggerFactory.getLogger(SourceRepository.class);
    private final String sourceSchema;

    public SourceRepository() {
        this.sourceSchema = AppConfigLoader.getDbConfig().sourceSchema();
    }
    public String getSourceSchema() { return sourceSchema; }

    /**
     * Lấy danh sách Connection từ Source
     */
    public List<TargetConnection> getAllConnections(Connection conn) throws SQLException {
        List<TargetConnection> list = new ArrayList<>();
        String sql = "SELECT * FROM " + sourceSchema + ".D_CONNECTION";

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetConnection(
                        rs.getLong("ID"),
                        rs.getString("URL"),
                        rs.getString("USER_NAME"),
                        rs.getString("PASS"),
                        rs.getString("DESCRIPTION"),
                        rs.getString("CONNECTION_NAME"),
                        null, // Driver name sẽ được detect lại ở Transformer
                        rs.getLong("ID_DB"),
                        rs.getString("PORT"),
                        rs.getString("TYPE_DB"),
                        rs.getTimestamp("INSERT_DATE")
                ));
            }
        }
        return list;
    }

    /**
     * Lấy dữ liệu Pipeline (Config, Command, Create)
     * ĐIỀU KIỆN: Chỉ lấy những Config nằm trong bảng History có trạng thái SUCCESS
     */
    public List<UnifiedSourceDTO> getValidPipelineData(Connection conn) throws SQLException {
        List<UnifiedSourceDTO> list = new ArrayList<>();

        // Sub-query để lọc ID Active từ History
        // Lưu ý: Bạn có thể đưa các tham số ngày tháng (20251101) ra file config nếu cần động
        String historyFilter = "SELECT DISTINCT ID_TABLE FROM " + sourceSchema + ".D_DB_2_HDFS_HISTORY " +
                "WHERE PRD_ID >= 20251120 AND PRD_ID <= 20251226 AND STATUS = 'SUCCESS'";

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        // 1. Config Info
        sql.append("  cfg.ID as CFG_ID, cfg.TABLE_NAME, cfg.LOCATION_PATH, cfg.DESCRIPTION as CFG_DESC, ");
        sql.append("  cfg.SOURCE, cfg.IMPORT_TYPE, cfg.OUTPUT_FORMAT, ");

        // 2. Command Info
        sql.append("  cmd.ID as CMD_ID, cmd.SQL_COMMAND as CMD_SQL, cmd.ID_CONNECTION, cmd.DESCRIPTION as CMD_DESC, ");
        sql.append("  cmd.FETCH_SIZE, cmd.USE_PARTITION, cmd.NUM_FIELDS, cmd.NUM_EXES, cmd.NUM_PARTS, ");
        sql.append("  cmd.USE_SUBPARTITION, cmd.PARAMS, cmd.SPLIT_COLUMN, cmd.SOURCE_TABLE, cmd.ID_DB as CMD_ID_DB, ");

        // 3. Create Command Info (Left Join vì có thể null)
        sql.append("  crt.ID as CRT_ID, crt.SQL_COMMAND as CRT_SQL, crt.DESCRIPTION as CRT_DESC, crt.ID_CONNECTION as CRT_CONN ");

        sql.append("FROM " + sourceSchema + ".D_DB_2_HDFS_CONFIG cfg ");
        sql.append("JOIN " + sourceSchema + ".D_DB_2_HDFS_COMMAND cmd ON cfg.ID_COMMAND = cmd.ID ");
        sql.append("LEFT JOIN " + sourceSchema + ".D_DB_2_HDFS_COMMAND_CREATE crt ON cfg.ID_COMMAND_CREATE = crt.ID ");

        // Áp dụng bộ lọc History
        sql.append("WHERE cfg.ID IN (").append(historyFilter).append(")");

        logger.info("Executing Pipeline Query (Active Only)...");

        try (PreparedStatement ps = conn.prepareStatement(sql.toString());
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new UnifiedSourceDTO(
                        // Config Args
                        rs.getLong("CFG_ID"),
                        rs.getString("TABLE_NAME"),
                        rs.getString("LOCATION_PATH"),
                        rs.getString("CFG_DESC"),
                        rs.getString("SOURCE"),
                        rs.getString("IMPORT_TYPE"),
                        rs.getString("OUTPUT_FORMAT"),

                        // Command Args
                        rs.getLong("CMD_ID"),
                        rs.getString("CMD_SQL"),
                        rs.getLong("FETCH_SIZE"),
                        rs.getLong("USE_PARTITION"),
                        rs.getLong("NUM_FIELDS"),
                        rs.getLong("NUM_EXES"),
                        rs.getLong("NUM_PARTS"),
                        rs.getLong("USE_SUBPARTITION"),
                        rs.getString("PARAMS"),
                        rs.getString("SPLIT_COLUMN"),
                        rs.getLong("CMD_ID_DB"),
                        rs.getString("CMD_DESC"),

                        // Create Args
                        rs.getObject("CRT_ID") != null ? rs.getLong("CRT_ID") : null,
                        rs.getString("CRT_SQL"),
                        rs.getString("CRT_DESC"),
                        rs.getObject("CRT_CONN") != null ? rs.getLong("CRT_CONN") : null,

                        // Connection ID (dùng chung cho luồng)
                        rs.getLong("ID_CONNECTION")
                ));
            }
        }
        return list;
    }

    /**
     * Lấy danh sách Time Param Config
     */
    public List<TargetTimeParam> getAllTimeParams(Connection conn) throws SQLException {
        List<TargetTimeParam> list = new ArrayList<>();
        String sql = "SELECT * FROM " + sourceSchema + ".D_TIME_PARAM_CONFIG";

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new TargetTimeParam(
                        rs.getString("NAME"),
                        rs.getInt("ADD_DAY"),
                        rs.getInt("ADD_MON"),
                        rs.getInt("ADD_YEAR"),
                        rs.getString("FORMAT"),
                        rs.getString("EXTEND_FORMAT"),
                        rs.getInt("ADD_MIN"),
                        rs.getInt("ADD_HOUR")
                ));
            }
        }
        return list;
    }

    public List<SourceConfig> getAllSourceConfig(Connection conn) throws SQLException {
        List<SourceConfig> list = new ArrayList<>();
        String sql = "SELECT ID, TABLE_NAME FROM " + sourceSchema + ".D_DB_2_HDFS_CONFIG";

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new SourceConfig(
                        rs.getLong("ID"),
                        rs.getString("TABLE_NAME")
                ));
            }
        }
        return list;
    }
}
