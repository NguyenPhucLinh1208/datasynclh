package com.viettel.sync.service;

import com.viettel.sync.core.SecurityUtils;
import com.viettel.sync.model.source.SourceConfig;
import com.viettel.sync.model.source.UnifiedSourceDTO;
import com.viettel.sync.model.target.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SyncLogicHelper {

    private static final Logger logger = LoggerFactory.getLogger(SyncLogicHelper.class);

    public record ConnectionAnalysisResult(
            List<TargetConnection> toInsert,
            Set<Long> fixIds,
            Set<Long> safeIds,
            List<String> logs
    ) {}

    public record PipelineSafeResult(
            Set<Long> safeCommandIds,
            Set<Long> safeConfigIds,
            Set<Long> safeCreateIds
    ) {}

    public static ConnectionAnalysisResult analyzeConnections(
            List<TargetConnection> sourceList,
            List<TargetConnection> targetList
    ) {
        List<TargetConnection> toInsert = new ArrayList<>();
        Set<Long> fixIds = new HashSet<>();
        Set<Long> safeIds = new HashSet<>();
        List<String> logs = new ArrayList<>();

        // Phân loại Source
        List<TargetConnection> srcFtp = new ArrayList<>();
        List<TargetConnection> srcJdbc = new ArrayList<>();
        for (TargetConnection c : sourceList) {
            if (DataTransformer.isFtpUrl(c.url())) srcFtp.add(c);
            else srcJdbc.add(c);
        }

        // Phân loại Target
        List<TargetConnection> tgtFtp = new ArrayList<>();
        List<TargetConnection> tgtJdbc = new ArrayList<>();
        for (TargetConnection c : targetList) {
            if (DataTransformer.isFtpUrl(c.url())) tgtFtp.add(c);
            else tgtJdbc.add(c);
        }

        // =====================================================================
        // PHẦN A: LOGIC JDBC
        // =====================================================================
        Map<Long, TargetConnection> targetJdbcMap = tgtJdbc.stream()
                .collect(Collectors.toMap(TargetConnection::id, i -> i));

        for (TargetConnection src : srcJdbc) {
            TargetConnection tgt = targetJdbcMap.get(src.id());
            TargetConnection readyToInsert = DataTransformer.transformConnection(src, tgt);

            // Case 1: NEW
            if (tgt == null) {
                toInsert.add(readyToInsert);
                continue;
            }

            // Case 2: EXIST (so sánh user/pass)
            boolean userMatch = Objects.equals(readyToInsert.userName(), tgt.userName());
            boolean passMatch = Objects.equals(readyToInsert.pass(), tgt.pass());

            if (userMatch && passMatch) {
                toInsert.add(readyToInsert);
            } else {
                fixIds.add(src.id());
                logs.add("⚠️ CONFLICT JDBC ID " + src.id() + ": Auth Diff");
            }
        }

        // SAFE check JDBC
        Set<Long> sourceJdbcIds = srcJdbc.stream().map(TargetConnection::id).collect(Collectors.toSet());
        for (TargetConnection tgt : tgtJdbc) {
            if (!sourceJdbcIds.contains(tgt.id())) {
                String tgtUrl = tgt.url() != null ? tgt.url().trim() : null;
                boolean notSafe = srcJdbc.stream().anyMatch(src -> {
                    String srcUrl = src.url() != null ? src.url().trim() : null;
                    if (!Objects.equals(normalizeJdbcUrl(srcUrl), normalizeJdbcUrl(tgtUrl))) return false;
                    if (!Objects.equals(SecurityUtils.decrypt(src.userName()), tgt.userName())) return false;
                    if (!Objects.equals(SecurityUtils.encrypt(SecurityUtils.decrypt(src.pass())), tgt.pass())) return false;
                    return true;
                });

                if (!notSafe) {
                    safeIds.add(tgt.id());
                    fixIds.add(tgt.id());
                } else {
                    Optional<Long> srcIdOpt = srcJdbc.stream()
                            .filter(src -> {
                                String srcUrl = src.url() != null ? src.url().trim() : null;
                                if (!Objects.equals(normalizeJdbcUrl(srcUrl), normalizeJdbcUrl(tgtUrl))) return false;
                                if (!Objects.equals(SecurityUtils.decrypt(src.userName()), tgt.userName())) return false;
                                if (!Objects.equals(SecurityUtils.encrypt(SecurityUtils.decrypt(src.pass())), tgt.pass())) return false;
                                return true;
                            })
                            .map(TargetConnection::id)
                            .findFirst();

                    if (srcIdOpt.isPresent()) {
                        logger.info("ℹ️ DUPLICATE JDBC Target ID " + tgt.id() + "URL " + tgt.url() + "user " + tgt.userName() +
                                " (same as Source ID " + srcIdOpt.get() + ")");
                    } else {
                        logger.info("ℹ️ DUPLICATE JDBC ID " + tgt.id() + " (same url/user/pass as source)");
                    }
                }
            }
        }

        // =====================================================================
        // PHẦN B: LOGIC FTP (Logic Mới - Đã sửa lỗi trùng ID)
        // =====================================================================
        // B1. Tạo danh sách Key của Source FTP để tra cứu
        Set<String> sourceFtpKeys = new HashSet<>();
        for (TargetConnection src : srcFtp) {
            sourceFtpKeys.add(getFtpKey(src));
        }

        // B2. Xử lý Target FTP trước
        for (TargetConnection tgt : tgtFtp) {
             String tgtKey = getTargetFtpKey(tgt);
            if (!sourceFtpKeys.contains(tgtKey)) {
                fixIds.add(tgt.id()); // Target Only -> giữ lại
            }
        }

        // B3. Xử lý Source FTP sau
        for (TargetConnection src : srcFtp) {
            TargetConnection readyToInsert = DataTransformer.transformConnection(src, null);

            if (fixIds.contains(src.id())) {
                logs.add(String.format("❌ FTP SKIP INSERT ID %d: ID này trùng với ID đang được giữ lại (Fixed/Safe). User=%s, pass=%s",
                        src.id(), src.userName(), src.pass()));
                continue;
            }

            toInsert.add(readyToInsert);
        }

        return new ConnectionAnalysisResult(toInsert, fixIds, safeIds, logs);
    }
    /**
     * Tính toán danh sách Safe ID cho Pipeline (Command, Config, Create)
     * Logic:
     * 1. Safe theo Connection (Connection được giữ lại -> Pipeline giữ lại).
     * 2. Safe theo Table Name (Bảng chỉ có ở Target -> Pipeline giữ lại).
     */
    public static PipelineSafeResult computePipelineSafeLists(
            Set<Long> fixConnectionIds,
            List<TargetCommand> allCommands,
            List<TargetConfig> allConfigs,
            List<SourceConfig> AllSourceData
    ) {
        // --- BƯỚC 1: Safe theo Connection ---
        Set<Long> safeCmdIds = allCommands.stream()
                .filter(c -> fixConnectionIds.contains(c.idConnection()))
                .map(TargetCommand::id)
                .collect(Collectors.toSet());

        Set<Long> safeCfgIds = allConfigs.stream()
                .filter(c -> safeCmdIds.contains(c.idCommand()))
                .map(TargetConfig::id)
                .collect(Collectors.toSet());

        // --- BƯỚC 2: Safe theo Table Name
        // Lọc ra các Config chưa được Safe ở bước 1
        List<TargetConfig> remainingConfigs = allConfigs.stream()
                .filter(c -> !safeCfgIds.contains(c.id()))
                .collect(Collectors.toList());

        if (!remainingConfigs.isEmpty() && AllSourceData != null) {
            // Tạo Set tên bảng của Source để tra cứu nhanh (Normalize: lowercase, no schema)
            Set<String> sourceTableNames = AllSourceData.stream()
                    .map(s -> normalizeTableName(s.tableName()))
                    .collect(Collectors.toSet());

            for (TargetConfig cfg : remainingConfigs) {
                String tgtTableName = normalizeTableName(cfg.tableName());

                // Nếu tên bảng Target KHÔNG tồn tại trong Source --> Đây là bảng làm tay --> Giữ lại
                if (!sourceTableNames.contains(tgtTableName)) {
                    safeCfgIds.add(cfg.id());

                    // Nếu Config được giữ, Command cha của nó cũng phải được giữ
                    if (cfg.idCommand() != null) {
                        safeCmdIds.add(cfg.idCommand());
                    }

                    logger.info("🛡️ KEEP TARGET-ONLY PIPELINE: ConfigId={} | Table={}", cfg.id(), tgtTableName);
                }
            }
        }

        // --- BƯỚC 3: Safe Create IDs ---
        // Create được giữ nếu Config trỏ tới nó được giữ
        Set<Long> safeCreateIds = allConfigs.stream()
                .filter(c -> safeCfgIds.contains(c.id()))
                .filter(c -> c.idCommandCreate() != null)
                .map(TargetConfig::idCommandCreate)
                .collect(Collectors.toSet());

        return new PipelineSafeResult(safeCmdIds, safeCfgIds, safeCreateIds);
    }

    // --- Helper Methods ---

    // Tạo key định danh FTP: URL|User
    private static String getFtpKey(TargetConnection c) {
        if (c == null) return "null|null";
        String u = c.url() != null ? c.url().trim() : "null";
        String n = SecurityUtils.decrypt(c.userName()) != null ? SecurityUtils.decrypt(c.userName()).trim() : "null";
        return u + "|" + n;
    }

    private static String getTargetFtpKey(TargetConnection c) {
        if (c == null) return "null|null";
        String u = c.url() != null ? c.url().trim() : "null";
        String n = c.userName() != null ? c.userName().trim() : "null";
        return u + "|" + n;
    }

    // Chuẩn hóa JDBC URL để so sánh
    private static String normalizeJdbcUrl(String url) {
        if (url == null) return null;
        return url.trim().replaceAll("\\s+", "").toLowerCase();
    }

    // Chuẩn hóa Table Name: Bỏ schema, lowercase
    private static String normalizeTableName(String raw) {
        if (raw == null) return "";
        String clean = raw.trim();
        // Lấy phần sau dấu chấm cuối cùng (nếu có)
        if (clean.contains(".")) {
            clean = clean.substring(clean.lastIndexOf(".") + 1);
        }
        return clean.toLowerCase();
    }
}