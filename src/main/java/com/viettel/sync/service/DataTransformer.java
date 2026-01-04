package com.viettel.sync.service;

import com.viettel.sync.core.SecurityUtils;
import com.viettel.sync.model.source.UnifiedSourceDTO;
import com.viettel.sync.model.target.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataTransformer {
    private static final Logger logger = LoggerFactory.getLogger(DataTransformer.class);

    // Pattern IP dùng chung
    private static final Pattern IP_PATTERN = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");

    // --- CÁC HÀM TIỆN ÍCH DÙNG CHUNG (PUBLIC) ---

    public static boolean isFtpUrl(String url) {
        if (url == null) return false;
        String trimmed = url.trim();

        String ipv4Regex = "^\\d{1,3}(\\.\\d{1,3}){3}$";

        boolean isIpv4Only = trimmed.matches(ipv4Regex);

        boolean containsLetter = trimmed.matches(".*[a-zA-Z].*");

        return isIpv4Only && !containsLetter;
    }


    public static String extractHostOrIp(String url) {
        if (url == null) return "";
        String cleanUrl = url.trim();

        Matcher ipMatcher = IP_PATTERN.matcher(cleanUrl);
        if (ipMatcher.matches()) return cleanUrl;

        if (cleanUrl.toLowerCase().startsWith("ftp://")) {
            try {
                URI uri = new URI(cleanUrl);
                return uri.getHost() != null ? uri.getHost() : cleanUrl;
            } catch (Exception e) {
                int start = cleanUrl.indexOf("://") + 3;
                int end = cleanUrl.indexOf("/", start);
                if (end == -1) end = cleanUrl.length();
                return cleanUrl.substring(start, end);
            }
        }
        return cleanUrl;
    }

    // --- CÁC HÀM LOGIC PRIVATE (GIỮ NGUYÊN TỪ FILE CŨ CỦA BẠN) ---

    private static String detectDriver(String url) {
        if (url == null) return "unknown.driver";
        String lowerUrl = url.toLowerCase().trim();
        if (lowerUrl.startsWith("jdbc:oracle")) return "oracle.jdbc.OracleDriver";
        if (lowerUrl.startsWith("jdbc:mysql")) return "com.mysql.cj.jdbc.Driver";
        if (lowerUrl.startsWith("jdbc:sqlserver")) return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        if (lowerUrl.startsWith("jdbc:postgresql")) return "org.postgresql.Driver";
        if (lowerUrl.startsWith("jdbc:hive2")) return "org.apache.hive.jdbc.HiveDriver";
        if (lowerUrl.startsWith("jdbc:mariadb")) return "org.mariadb.jdbc.Driver";
        if (lowerUrl.startsWith("jdbc:clickhouse")) return "com.clickhouse.jdbc.ClickHouseDriver";
        if (lowerUrl.startsWith("jdbc:db2")) return "com.ibm.db2.jcc.DB2Driver";
        return "unknown.driver";
    }

    private static String extractDbName(String rawUrl) {
        if (rawUrl == null) return null;
        String url = rawUrl.replace("\n", " ").replaceAll("\\s+", " ").trim();

        Matcher mOracle = Pattern.compile("(?:SERVICE_NAME|SID)\\s*=\\s*([^\\s)]+)", Pattern.CASE_INSENSITIVE).matcher(url);
        if (mOracle.find()) return mOracle.group(1);

        Matcher mOracleEasy = Pattern.compile("jdbc:oracle:.*@(?:/{0,2})[^:/]+:\\d+[:/]([^:/?]+)", Pattern.CASE_INSENSITIVE).matcher(url);
        if (mOracleEasy.find()) return mOracleEasy.group(1);

        if (url.contains("://")) {
            Matcher mGeneric = Pattern.compile("://[^/]+/([^?]+)", Pattern.CASE_INSENSITIVE).matcher(url);
            if (mGeneric.find()) return mGeneric.group(1);
        }

        Matcher mSql = Pattern.compile("(?:databaseName|database)\\s*=\\s*([^;]+)", Pattern.CASE_INSENSITIVE).matcher(url);
        if (mSql.find()) return mSql.group(1);

        return null;
    }

    private static String normalizeName(String rawName) {
        if (rawName == null) return "unknown";
        String name = rawName.trim().toLowerCase();
        if (name.endsWith("dg")) name = name.substring(0, name.length() - 2);
        name = name.replaceAll("(?:v\\d+|_\\d+|\\d+)$", "");
        return name;
    }

    // --- HÀM TRANSFORM CONNECTION (ĐÃ SỬA LOGIC FTP GIỮ PASSWORD) ---

    public static TargetConnection transformConnection(TargetConnection src, TargetConnection existingTarget) {
        if (src == null) return null;

        String url = src.url() == null ? "" : src.url().trim();
        boolean isFtp = isFtpUrl(url); // Gọi hàm chung

        String finalUser;
        String finalPass;
        String finalDriver;
        String finalConnName;
        finalUser = SecurityUtils.decrypt(src.userName());
        finalPass = SecurityUtils.encrypt(SecurityUtils.decrypt(src.pass()));

        if (isFtp) {
            // LOGIC FTP
            finalDriver = "";
            // Tự động sinh tên ftp_IP tại đây
            String host = extractHostOrIp(url);
            finalConnName = "ftp_" + host;

        } else {
            // LOGIC JDBC
            finalDriver = detectDriver(url);
            String tempName = extractDbName(url);
            finalConnName = normalizeName(tempName);
            if (finalConnName.equals("unknown")) finalConnName = src.connectionName();

            // JDBC: Luôn đồng bộ từ Source
            finalUser = SecurityUtils.decrypt(src.userName());
            finalPass = SecurityUtils.encrypt(SecurityUtils.decrypt(src.pass()));
        }

        return new TargetConnection(
                src.id(), url, finalUser, finalPass, src.description(),
                finalConnName, finalDriver, src.idDb(), src.port(), src.typeDb(),
                new Timestamp(System.currentTimeMillis())
        );
    }


    public static TargetConfig transformConfig(UnifiedSourceDTO src, String targetConnName) {
        String rawTableName = src.tableName();
        String cleanTableName = rawTableName;
        if (cleanTableName != null && cleanTableName.contains(".")) {
            cleanTableName = cleanTableName.substring(cleanTableName.lastIndexOf(".") + 1);
        }
        if (cleanTableName == null || cleanTableName.isBlank()) cleanTableName = "unknown_table" + src.cfgId();

        String finalTableName = "ingestion." + cleanTableName.toLowerCase();

        String basePath = "/raw_data/" + targetConnName.toLowerCase() + "/" + cleanTableName.toLowerCase();

        String sourcePath = src.locationPath();

        String finalPath =  resolveFinalPath(sourcePath, basePath, src.cfgId());


        return new TargetConfig(
                src.cfgId(), src.cmdId(), src.createId(), finalTableName, finalPath,
                null, src.source(), src.cfgDesc(), 0L, 25251325L, 900L,
                new Timestamp(System.currentTimeMillis()), "PARQUET", src.importType()
        );
    }

    private static String resolveFinalPath(String sourcePath, String baseTargetPath, Long cfgId) {
        if (sourcePath == null || sourcePath.trim().isEmpty()) return baseTargetPath;

        String rawPath = sourcePath.trim().replaceAll("^hdfs://[^/]+", "");

        if (rawPath.startsWith("/")) rawPath = rawPath.substring(1);
        if (rawPath.endsWith("/")) rawPath = rawPath.substring(0, rawPath.length() - 1);

        String[] segments = rawPath.split("/");
        long dollarCount = rawPath.chars().filter(ch -> ch == '$').count();

        if (dollarCount == 0) {
            return baseTargetPath;
        }

        // Phân tích xem có phải tất cả các đoạn chứa $ đều có dạng key=value không
        boolean isAllKeyValue = true;
        int firstDollarIndex = -1;

        for (int i = 0; i < segments.length; i++) {
            if (segments[i].contains("$")) {
                if (firstDollarIndex == -1) firstDollarIndex = i;
                if (!segments[i].matches(".*=.*\\$\\{.*}.*")) {
                    isAllKeyValue = false;
                }
            }
        }

        // CASE 1: Key-Value Partition (x=${}/y=${}/...)
        // Điều kiện: Có ít nhất 1 $, và TẤT CẢ các segment chứa $ phải có dạng key=value
        if (isAllKeyValue) {

            boolean foundDynamic = false;
            boolean foundStaticAfterDynamic = false;

            StringBuilder pathSuffix = new StringBuilder();

            // Duyệt từ vị trí xuất hiện biến $ đầu tiên
            for (int i = firstDollarIndex; i < segments.length; i++) {
                String seg = segments[i];
                boolean isDynamic = seg.contains("$");

                if (isDynamic) {
                    if (foundStaticAfterDynamic) {
                        logger.warn("⚠️ [CONFIG ID {}] Path Invalid: Static folder '{}' chen giữa các partition. Path: {}", cfgId, segments[i-1], sourcePath);
                        return sourcePath; // Invalid: /x=${}/static/y=${}
                    }
                    foundDynamic = true;
                } else {
                    if (foundDynamic) {
                        foundStaticAfterDynamic = true;
                        // Kiểm tra nếu sau static này vẫn còn segment nữa -> Sai (vì chỉ cho phép 1 cấp cuối)
                        if (i < segments.length - 1) {
                            logger.warn("⚠️ [CONFIG ID {}] Path Invalid: Quá nhiều cấp thư mục tĩnh sau partition. Path: {}", cfgId, sourcePath);
                            return sourcePath; // Invalid: /x=${}/y/z
                        }
                    }
                }
                pathSuffix.append("/").append(seg);
            }

            return baseTargetPath + pathSuffix.toString();
        }

        // CASE 2: Single Implicit Partition (/${}/...)
        // Điều kiện: Chỉ có duy nhất 1 dấu $ và KHÔNG nằm trong cấu trúc key=value (đã check ở trên)
        if (dollarCount == 1) {
            // Tìm vị trí của segment chứa $
            int dollarIndex = -1;
            for (int i = 0; i < segments.length; i++) {
                if (segments[i].contains("$")) {
                    dollarIndex = i;
                    break;
                }
            }

            // Segment chứa $ không được có dấu = (theo logic TH2)
            if (segments[dollarIndex].contains("=")) {
                // Đây là trường hợp lạ (có = nhưng lại rớt xuống đây), log warn
                logger.warn("⚠️ [CONFIG ID {}] Path Ambiguous: 1 $ có dấu '=' nhưng không khớp logic TH1. Path: {}", cfgId, sourcePath);
                return sourcePath;
            }

            // Logic: Cho phép có hoặc không có thêm MỘT cấp duy nhất phía sau
            // Tức là dollarIndex phải là last hoặc (last - 1)
            if (dollarIndex == segments.length - 1) {
                // Dạng: .../${YYYYMMDD}
                return baseTargetPath + "/partition=" + segments[dollarIndex];
            } else if (dollarIndex == segments.length - 2) {
                // Dạng: .../${YYYYMMDD}/file.txt
                return baseTargetPath + "/partition=" + segments[dollarIndex] + "/" + segments[dollarIndex + 1];
            } else {
                // Dạng: .../${}/a/b/c -> Sai
                logger.warn("⚠️ [CONFIG ID {}] Path Invalid: Quá nhiều cấp sau biến đơn (${}). Path: {}", cfgId, sourcePath);
                return sourcePath;
            }
        }

        // CASE 4: Các trường hợp còn lại (Ví dụ: 2 dấu $ nhưng không có =, dạng /${}/${})
        logger.warn("⚠️ [CONFIG ID {}] Path Skip: Cấu trúc quá phức tạp ({} $). Path: {}", cfgId, dollarCount, sourcePath);
        return sourcePath;
    }



    public static String normalizePathForMap(String rawPath) {
        if (rawPath == null) return "";
        String clean = rawPath.replaceAll("^hdfs://[^/]+", "");
        int cutIdx = -1;
        if (clean.contains("$")) cutIdx = clean.indexOf("$");
        else if (clean.contains("partition=")) cutIdx = clean.indexOf("partition=");
        if (cutIdx != -1) {
            int slashIdx = clean.lastIndexOf("/", cutIdx);
            if (slashIdx > 0) clean = clean.substring(0, slashIdx);
        }
        if (clean.endsWith("/")) clean = clean.substring(0, clean.length() - 1);
        return clean;
    }

    public static TargetCommand transformCommand(UnifiedSourceDTO src, String oldMaskColumn) {
        return new TargetCommand(
                src.cmdId(), src.connId(), src.fetchSize(), src.usePartition(), src.numFields(),
                src.numExes(), src.numParts(), src.cmdDesc(), src.useSubpartition(), src.params(),
                src.cmdIdDb(), src.splitColumn(), oldMaskColumn, new Timestamp(System.currentTimeMillis()), src.cmdSql()
        );
    }

    public static TargetCreate transformCreate(UnifiedSourceDTO src, Map<String, String> globalPathMap) {
        if (src.createId() == null || src.createSql() == null) return null;

        String rawSql = src.createSql();

        // --- Bước 1: Chuẩn hóa tên bảng ---
        String regexTable = "(?i)(TABLE\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?)" +
                "([a-zA-Z0-9_]+)";
        Matcher matcher = Pattern.compile(regexTable).matcher(rawSql);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String tableName = matcher.group(2);
            String newTableName = "ingestion." + tableName.toLowerCase();
            matcher.appendReplacement(sb, matcher.group(1) + newTableName);
        }
        matcher.appendTail(sb);
        rawSql = sb.toString();

        // --- Bước 2: Chuẩn hóa format lưu trữ ---
        String regexFormat = "(?is)((ROW\\s+FORMAT\\s+DELIMITED(\\s+FIELDS\\s+TERMINATED\\s+BY\\s*'[^']*')?(\\s+LINES\\s+TERMINATED\\s+BY\\s*'[^']*')?)"
                + "|(STORED\\s+AS\\s+TEXTFILE)"
                + "|(STORED\\s+AS\\s+INPUTFORMAT\\s+'[^']*'\\s+OUTPUTFORMAT\\s+'[^']*')"
                + "|(ROW\\s+FORMAT\\s+SERDE\\s+'[^']*'))";
        String tempSql = rawSql.replaceAll(regexFormat, "STORED AS PARQUET");
        tempSql = tempSql.replaceAll("(?i)(STORED AS PARQUET\\s*)+", "STORED AS PARQUET ");

        // --- Bước 3: Tìm toàn bộ location path ---
        // Regex: bắt cụm LOCATION '...'
        Pattern pathPattern = Pattern.compile("(?i)LOCATION\\s+'([^']+)'");
        Matcher pathMatcher = pathPattern.matcher(tempSql);

        StringBuffer sbPaths = new StringBuffer();
        while (pathMatcher.find()) {
            String fullPath = pathMatcher.group(1); // lấy nội dung trong dấu nháy

            // --- Bước 4: Kiểm tra path có khớp với globalPathMap không ---
            String resolvedPath = fullPath;
            for (Map.Entry<String, String> entry : globalPathMap.entrySet()) {
                String oldBase = entry.getKey();
                String newBase = entry.getValue();
                if (fullPath.contains(oldBase)) {
                    resolvedPath = resolveFinalPath(fullPath, newBase, src.createId());
                    break; // chỉ thay thế lần đầu khớp
                }
            }

            // --- Bước 5: Thay thế vào SQL ---
            pathMatcher.appendReplacement(sbPaths, "LOCATION '" + Matcher.quoteReplacement(resolvedPath) + "'");
        }
        pathMatcher.appendTail(sbPaths);
        tempSql = sbPaths.toString();

        return new TargetCreate(
                src.createId(), src.createIdConnection(), src.createDesc(),
                new Timestamp(System.currentTimeMillis()), tempSql
        );
    }

    public static TargetCreate generateDropPartitionCommand(TargetConfig cfg, long nextCreateId) {
        if (cfg == null || cfg.tableName() == null) return null;
        String sql = "ALTER TABLE " + cfg.tableName() + " DROP IF EXISTS PARTITION(partition=\"${YYYYMMDD:MM-6}\")";
        return new TargetCreate(
                nextCreateId,
                null,
                cfg.tableName(),
                null,
                sql
        );
    }

    public static TargetClean generateCleanFromConfig(TargetConfig cfg) {
        if (cfg == null || cfg.locationPath() == null) return null;
        String path = cfg.locationPath();
        long dollarCount = path.chars().filter(ch -> ch == '$').count();
        if (dollarCount != 1) return null;
        String newPath = path.replaceAll("\\$\\{[^}]+\\}", "\\${YYYYMMDD:MM-6}");
        return new TargetClean(
                newPath,
                cfg.tableName(),
                0L,
                null,
                new Timestamp(System.currentTimeMillis())
        );
    }
}