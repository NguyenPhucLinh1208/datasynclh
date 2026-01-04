package com.viettel.sync.model.target;

import com.viettel.sync.core.Annotations.*;
import java.sql.Timestamp;

@Table(name = "D_DB_2_HDFS_CONFIG")
public record TargetConfig(
        @Id @Column(name = "ID") Long id,
        @Column(name = "ID_COMMAND") Long idCommand,
        @Column(name = "ID_COMMAND_CREATE") Long idCommandCreate,
        @Column(name = "TABLE_NAME") String tableName,
        @Column(name = "LOCATION_PATH") String locationPath,
        @Column(name = "REMOVE_PATH") String removePath,
        @Column(name = "SOURCE") String source,
        @Column(name = "DESCRIPTION") String description,
        @Column(name = "IS_ACTIVE") Long isActive,
        @Column(name = "ID_GROUP") Long idGroup,
        @Column(name = "MAX_TIME") Long maxTime,
        @Column(name = "INSERT_DATE") Timestamp insertDate,
        @Column(name = "OUTPUT_FORMAT") String outputFormat,
        @Column(name = "IMPORT_TYPE") String importType
) {}