package com.viettel.sync.model.target;
import com.viettel.sync.core.Annotations.*;
import java.sql.Timestamp;

@Table(name = "D_DB_2_HDFS_COMMAND_CREATE")
public record TargetCreate(
        @Id @Column(name = "ID") Long id,
        @Column(name = "ID_CONNECTION") Long idConnection, // Mới
        @Column(name = "DESCRIPTION") String description,
        @Column(name = "INSERT_DATE") Timestamp insertDate,
        @Column(name = "SQL_COMMAND") String sqlCommand
) {}
