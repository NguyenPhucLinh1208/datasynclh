package com.viettel.sync.model.source;

import com.viettel.sync.core.Annotations.*;

@Table(name = "D_DB_2_HDFS_CONFIG")
public record SourceConfig(
        @Id @Column(name = "ID") Long id,
        @Column(name = "TABLE_NAME") String tableName
) {}