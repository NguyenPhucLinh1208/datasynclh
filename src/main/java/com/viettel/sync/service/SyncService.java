package com.viettel.sync.service;

import com.viettel.sync.config.DatabaseUtils;
import com.viettel.sync.model.source.SourceConfig;
import com.viettel.sync.model.source.UnifiedSourceDTO;
import com.viettel.sync.model.target.*;
import com.viettel.sync.repository.SourceRepository;
import com.viettel.sync.repository.TargetRepository;
import com.viettel.sync.service.SyncLogicHelper.ConnectionAnalysisResult;
import com.viettel.sync.service.SyncLogicHelper.PipelineSafeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class SyncService {
    private static final Logger logger = LoggerFactory.getLogger(SyncService.class);
    private final SourceRepository sourceRepo = new SourceRepository();
    private final TargetRepository targetRepo = new TargetRepository();

    public void runSync() {
        Connection conn = null;
        try {
            conn = DatabaseUtils.getConnection();
            conn.setAutoCommit(false); // Bắt đầu Transaction
            logger.info("========== BẮT ĐẦU ĐỒNG BỘ (SAFE TABLE CHECK VERSION) ==========");

            // 1. Đồng bộ TimeParam
            syncTimeParam(conn);

            // 2. Đồng bộ Connections
            Set<Long> fixConnectionIds = syncConnections(conn);

            // 3. Đồng bộ Pipeline (Command, Config, Create, Clean + Auto Gen Logic)
            // Truyền fixConnectionIds để xác định các Pipeline thuộc Connection an toàn
            syncPipeline(conn, fixConnectionIds);

            conn.commit();
            logger.info("✅ ĐỒNG BỘ THÀNH CÔNG TOÀN BỘ!");
        } catch (Exception e) {
            logger.error("❌ LỖI NGHIÊM TRỌNG, ROLLBACK!", e);
            try {
                if (conn != null) conn.rollback();
            } catch (Exception ex) {
                logger.error("Lỗi khi rollback", ex);
            }
        } finally {
            DatabaseUtils.closeConnection(conn);
        }
    }

    private void syncTimeParam(Connection conn) throws Exception {
        logger.info("--- 1. SYNC TIME PARAM ---");
        List<TargetTimeParam> sourceList = sourceRepo.getAllTimeParams(conn);
        List<TargetTimeParam> targetList = targetRepo.getAllTargetTimeParams(conn);

        Set<String> targetNames = targetList.stream()
                .map(t -> t.name().trim().toLowerCase())
                .collect(Collectors.toSet());

        List<TargetTimeParam> toInsert = sourceList.stream()
                .filter(item -> !targetNames.contains(item.name().trim().toLowerCase()))
                .collect(Collectors.toList());

        if (!toInsert.isEmpty()) {
            targetRepo.timeRepo.insertBatch(conn, targetRepo.getTargetSchema(), toInsert);
            logger.info("   -> [INSERT] Đã thêm {} TimeParam.", toInsert.size());
        }
    }

    private Set<Long> syncConnections(Connection conn) throws Exception {
        logger.info("--- 2. SYNC CONNECTIONS ---");

        List<TargetConnection> sourceRaw = sourceRepo.getAllConnections(conn);
        List<TargetConnection> targetRaw = targetRepo.getAllTargetConnections(conn);

        // Gọi Helper tính toán logic (Pure Logic)
        ConnectionAnalysisResult result = SyncLogicHelper.analyzeConnections(sourceRaw, targetRaw);

        if (!result.logs().isEmpty()) {
            logger.warn("⚠️ PHÁT HIỆN {} XUNG ĐỘT/VẤN ĐỀ:", result.logs().size());
            result.logs().forEach(logger::warn);
        }

        // D1. DELETE: Xóa các Connection không nằm trong danh sách Fix (Fix = Safe hoặc Target Only được giữ lại)
        List<Long> idsToDelete = targetRaw.stream()
                .map(TargetConnection::id)
                .filter(id -> !result.fixIds().contains(id))
                .collect(Collectors.toList());

        if (!idsToDelete.isEmpty()) {
            targetRepo.connectionRepo.deleteByIds(conn, targetRepo.getTargetSchema(), TargetConnection.class, idsToDelete);
            logger.info("   -> [DELETE] Đã xóa {} connection cũ.", idsToDelete.size());
        }

        // D2. INSERT/UPDATE: Ghi các Connection mới hoặc ghi đè từ Source
        if (!result.toInsert().isEmpty()) {
            targetRepo.connectionRepo.insertBatch(conn, targetRepo.getTargetSchema(), result.toInsert());
            logger.info("   -> [INSERT/UPDATE] Đã ghi {} connection.", result.toInsert().size());
        }

        // D3. UPDATE INFO SAFE ID: Cập nhật thông tin Connection Name/Driver cho các ID Safe (giữ ID, update info)
        for (Long safeId : result.safeIds()) {
            targetRaw.stream().filter(t -> t.id().equals(safeId)).findFirst().ifPresent(tgt -> {
                TargetConnection normalized = DataTransformer.transformConnection(tgt, tgt);
                try {
                    targetRepo.connectionRepo.updateConnectionInfo(
                            conn,
                            targetRepo.getTargetSchema(),
                            TargetConnection.class,
                            safeId,
                            normalized.connectionName(),
                            normalized.driverName()
                    );
                } catch (Exception e) {
                    logger.error("Lỗi update Safe ID {}", safeId, e);
                }
            });
        }
        logger.info("Danh sách ID thuộc nhóm safe (chỉ có ở target) {}", result.safeIds());
        logger.info("Danh sách ID thuộc nhóm fix {}", result.fixIds());

        return result.fixIds();
    }

    private void syncPipeline(Connection conn, Set<Long> fixConnectionIds) throws Exception {
        logger.info("--- 3. SYNC PIPELINE (Auto Drop & Clean Logic + Check Table Safe) ---");

        // A. Lấy dữ liệu Source (để dùng cho logic check Safe Table Name)
        List<SourceConfig> allConfigData = sourceRepo.getAllSourceConfig(conn);
        List<UnifiedSourceDTO> validData = sourceRepo.getValidPipelineData(conn);

        // B. Lấy dữ liệu Target hiện tại
        List<TargetCommand> allCmds = targetRepo.getAllTargetCommands(conn);
        List<TargetConfig> allConfigs = targetRepo.getAllTargetConfigs(conn);
        List<TargetCreate> allCreates = targetRepo.getAllTargetCreates(conn);

        // C. Tính toán Safe List (Truyền thêm validData vào để check Table Name)
        PipelineSafeResult safeLists = SyncLogicHelper.computePipelineSafeLists(
                fixConnectionIds, allCmds, allConfigs, allConfigData
        );

        logger.info("   -> Safe Items: \n ID COMMAND {} = {} \n ID CONFIG {} ={}, \n ID COMMAND CREATE {} ={}",
                safeLists.safeCommandIds().size(), safeLists.safeCommandIds(), safeLists.safeConfigIds().size(),
                safeLists.safeConfigIds(), safeLists.safeCreateIds().size(), safeLists.safeCreateIds());

        // D. Xóa dữ liệu cũ (Trừ Safe List)
        targetRepo.cleanRepo.truncate(conn, targetRepo.getTargetSchema(), TargetClean.class);

        List<Long> delCfgIds = filterOut(allConfigs, safeLists.safeConfigIds());
        if (!delCfgIds.isEmpty()) targetRepo.configRepo.deleteByIds(conn, targetRepo.getTargetSchema(), TargetConfig.class, delCfgIds);

        List<Long> delCmdIds = filterOutCmd(allCmds, safeLists.safeCommandIds());
        if (!delCmdIds.isEmpty()) targetRepo.commandRepo.deleteByIds(conn, targetRepo.getTargetSchema(), TargetCommand.class, delCmdIds);

        List<Long> delCreateIds = allCreates.stream()
                .map(TargetCreate::id)
                .filter(id -> !safeLists.safeCreateIds().contains(id))
                .collect(Collectors.toList());
        if (!delCreateIds.isEmpty()) targetRepo.createRepo.deleteByIds(conn, targetRepo.getTargetSchema(), TargetCreate.class, delCreateIds);

        // E. Chuẩn bị dữ liệu Insert mới (Chỉ insert những cái không nằm trong Safe List)
        List<TargetConnection> currentTargetConns = targetRepo.getAllTargetConnections(conn);

        Map<Long, String> connNameMap = currentTargetConns.stream()
                .collect(Collectors.toMap(
                        TargetConnection::id,
                        c -> (c.connectionName() != null && !c.connectionName().isBlank()) ? c.connectionName() : "unknown_" + c.id(),
                        (existing, replacement) -> existing
                ));

        long nextCreateId = 1;
        String targetSchema = targetRepo.getTargetSchema();
        String sourceSchema = sourceRepo.getSourceSchema();

        try (Statement stmt = conn.createStatement()) {
            String sql1 = "SELECT MAX(ID) FROM " + targetSchema + ".D_DB_2_HDFS_COMMAND_CREATE";
            String sql2 = "SELECT MAX(ID) FROM " + sourceSchema + ".D_DB_2_HDFS_COMMAND_CREATE";

            long maxId1 = 0;
            long maxId2 = 0;

            try (java.sql.ResultSet rs1 = stmt.executeQuery(sql1)) {
                if (rs1.next()) {
                    maxId1 = rs1.getLong(1);
                    if (rs1.wasNull()) maxId1 = 0;
                }
            }

            try (java.sql.ResultSet rs2 = stmt.executeQuery(sql2)) {
                if (rs2.next()) {
                    maxId2 = rs2.getLong(1);
                    if (rs2.wasNull()) maxId2 = 0;
                }
            }

            long maxId = Math.max(maxId1, maxId2);
            nextCreateId = maxId + 1;
        }

        List<TargetCommand> insCmd = new ArrayList<>();
        List<TargetConfig> insCfg = new ArrayList<>();
        List<TargetCreate> insCreate = new ArrayList<>();
        List<TargetClean> insClean = new ArrayList<>();

        Map<Long, String> existingMaskMap = allCmds.stream()
                .collect(Collectors.toMap(TargetCommand::id, c -> c.maskColumn() != null ? c.maskColumn() : "", (k1, k2) -> k1));

        Map<String, String> globalPathMap = new HashMap<>();
        Set<Long> processedCfgIds = new HashSet<>();

        // --- VÒNG LẶP XỬ LÝ CHÍNH ---
        for (UnifiedSourceDTO src : validData) {
            // [QUAN TRỌNG] Nếu Config này đã được Safe (Giữ lại do Table Name hoặc Connection Safe)
            // Thì KHÔNG ĐƯỢC Insert lại từ Source nữa để tránh trùng lặp.
            if (safeLists.safeConfigIds().contains(src.cfgId())) continue;

            if (processedCfgIds.contains(src.cfgId())) continue;

            String targetConnName = connNameMap.getOrDefault(src.connId(), "unknown_conn_" + src.connId());

            // 1. Transform Config
            TargetConfig newCfg = DataTransformer.transformConfig(src, targetConnName);
            if (newCfg == null) continue;

            insCfg.add(newCfg);
            processedCfgIds.add(src.cfgId());

            // 2. Build Global Map for SQL Replacement
            String oldPathClean = DataTransformer.normalizePathForMap(src.locationPath());
            String newPathClean = DataTransformer.normalizePathForMap(newCfg.locationPath());
            if (!oldPathClean.isEmpty() && !newPathClean.isEmpty()) {
                globalPathMap.put(oldPathClean, newPathClean);
            }

            // a. Sinh lệnh Drop Partition (vào bảng Create)
            TargetCreate dropCmd = DataTransformer.generateDropPartitionCommand(newCfg, nextCreateId);
            if (dropCmd != null) {
                insCreate.add(dropCmd);
                nextCreateId++;
            }

            // b. Sinh lệnh Clean Folder (vào bảng Clean)
            TargetClean cleanCmd = DataTransformer.generateCleanFromConfig(newCfg);
            if (cleanCmd != null) {
                insClean.add(cleanCmd);
            }
        }

        logger.info("   -> Global Path Map: {} entries", globalPathMap.size());

        // --- XỬ LÝ COMMAND & CREATE GỐC TỪ SOURCE ---
        Set<Long> processedCmdIds = new HashSet<>();
        Set<Long> processedCreateIds = new HashSet<>();

        for (UnifiedSourceDTO src : validData) {
            // COMMAND: Chỉ insert nếu chưa Safe
            if (!safeLists.safeCommandIds().contains(src.cmdId()) && !processedCmdIds.contains(src.cmdId())) {
                String oldMask = existingMaskMap.get(src.cmdId());
                insCmd.add(DataTransformer.transformCommand(src, oldMask));
                processedCmdIds.add(src.cmdId());
            }

            // CREATE: Chỉ insert nếu chưa Safe
            if (src.createId() != null && !safeLists.safeCreateIds().contains(src.createId()) && !processedCreateIds.contains(src.createId())) {
                TargetCreate tc = DataTransformer.transformCreate(src, globalPathMap);
                if (tc != null && tc.sqlCommand() != null) {
                    insCreate.add(tc);
                }
                processedCreateIds.add(src.createId());
            }
        }

        // F. INSERT BATCH (DB sẽ tự sinh ID cho các bản ghi auto-gen)
        if (!insCmd.isEmpty()) targetRepo.commandRepo.insertBatch(conn, targetRepo.getTargetSchema(), insCmd);
        if (!insCfg.isEmpty()) targetRepo.configRepo.insertBatch(conn, targetRepo.getTargetSchema(), insCfg);
        if (!insCreate.isEmpty()) targetRepo.createRepo.insertBatch(conn, targetRepo.getTargetSchema(), insCreate);
        if (!insClean.isEmpty()) targetRepo.cleanRepo.insertBatch(conn, targetRepo.getTargetSchema(), insClean);

        logger.info("   -> [INSERT NEW] Cmd={}, Cfg={}, Create={}, Clean={}",
                insCmd.size(), insCfg.size(), insCreate.size(), insClean.size());

        // G. BƯỚC CẬP NHẬT LIÊN KẾT (MAGIC STEP)
        // Join bảng Clean với Created qua Description để update ID
        logger.info("   -> [POST-PROCESS] Linking Auto-Generated Clean & Drop Commands...");

        String sqlUpdateLink =
                "UPDATE " + targetSchema + ".D_CLEAN_FOLDER clean " +
                        "SET ID_COMMAND_CREATE = ( " +
                        "    SELECT cmd.ID " +
                        "    FROM " + targetSchema + ".D_DB_2_HDFS_COMMAND_CREATE cmd " +
                        "    WHERE cmd.DESCRIPTION = clean.DESCRIPTION " +
                        "    AND cmd.SQL_COMMAND LIKE 'ALTER TABLE%' " +
                        "    FETCH FIRST 1 ROWS ONLY " +
                        ") " +
                        "WHERE clean.ID_COMMAND_CREATE IS NULL " +
                        "AND EXISTS (SELECT 1 FROM " + targetSchema + ".D_DB_2_HDFS_COMMAND_CREATE c WHERE c.DESCRIPTION = clean.DESCRIPTION)";

        try (Statement stmt = conn.createStatement()) {
            int rows = stmt.executeUpdate(sqlUpdateLink);
            logger.info("   -> [LINKED] Đã cập nhật liên kết cho {} bản ghi Clean.", rows);
        }
    }

    // Helpers
    private List<Long> filterOut(List<TargetConfig> all, Set<Long> safeIds) {
        return all.stream().map(TargetConfig::id).filter(id -> !safeIds.contains(id)).collect(Collectors.toList());
    }
    private List<Long> filterOutCmd(List<TargetCommand> all, Set<Long> safeIds) {
        return all.stream().map(TargetCommand::id).filter(id -> !safeIds.contains(id)).collect(Collectors.toList());
    }
}