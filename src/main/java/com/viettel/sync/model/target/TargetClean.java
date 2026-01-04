package com.viettel.sync.model.target;
import com.viettel.sync.core.Annotations.*;
import java.sql.Timestamp;

@Table(name = "D_CLEAN_FOLDER")
public record TargetClean(
        @Column(name = "FOLDER") String folder,
        @Column(name = "DESCRIPTION") String description,
        @Column(name = "IS_ACTIVE") Long isActive,
        @Column(name = "ID_COMMAND_CREATE") Long idCommandCreate,
        @Column(name = "INSERT_DATE") Timestamp insertDate
) {}
