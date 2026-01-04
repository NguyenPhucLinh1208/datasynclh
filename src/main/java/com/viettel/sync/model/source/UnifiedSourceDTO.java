package com.viettel.sync.model.source;

public record UnifiedSourceDTO(
        // --- Config Data ---
        Long cfgId,
        String tableName,
        String locationPath,
        String cfgDesc,
        String source,          // Mới: Lấy nguyên gốc từ Source
        String importType,      // Mới: Lấy nguyên gốc từ Source
        String outputFormat,    // Mới

        // --- Command Data ---
        Long cmdId,
        String cmdSql,
        Long fetchSize,         // Mới
        Long usePartition,      // Mới
        Long numFields,         // Mới
        Long numExes,           // Mới
        Long numParts,          // Mới
        Long useSubpartition,   // Mới
        String params,          // Mới
        String splitColumn,     // Mới
        Long cmdIdDb,           // Mới
        String cmdDesc,         // Mới: Description của Command

        // --- Create Data ---
        Long createId,
        String createSql,
        String createDesc,
        Long createIdConnection,// Mới

        // --- Connection Info ---
        Long connId
) {}
