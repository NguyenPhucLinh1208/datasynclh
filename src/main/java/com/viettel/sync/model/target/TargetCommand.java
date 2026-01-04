package com.viettel.sync.model.target;
import com.viettel.sync.core.Annotations.*;
import java.sql.Timestamp;

@Table(name = "D_DB_2_HDFS_COMMAND")
public record TargetCommand(
        @Id @Column(name = "ID") Long id,
        @Column(name = "ID_CONNECTION") Long idConnection,
        @Column(name = "FETCH_SIZE") Long fetchSize,
        @Column(name = "USE_PARTITION") Long usePartition,
        @Column(name = "NUM_FIELDS") Long numFields,
        @Column(name = "NUM_EXES") Long numExes,
        @Column(name = "NUM_PARTS") Long numParts,
        @Column(name = "DESCRIPTION") String description,
        @Column(name = "USE_SUBPARTITION") Long useSubpartition,
        @Column(name = "PARAMS") String params,
        @Column(name = "ID_DB") Long idDb,
        @Column(name = "SPLIT_COLUMN") String splitColumn,
        @Column(name = "MASK_COLUMN") String maskColumn, // Giữ từ Target
        @Column(name = "INSERT_DATE") Timestamp insertDate,
        @Column(name = "SQL_COMMAND") String sqlCommand
) {}
