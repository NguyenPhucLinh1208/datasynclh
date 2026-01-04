package com.viettel.sync.model.target;
import com.viettel.sync.core.Annotations.*;
import java.sql.Timestamp;

@Table(name = "D_CONNECTION")
public record TargetConnection(
        @Id @Column(name = "ID") Long id,
        @Column(name = "URL") String url,
        @Column(name = "USER_NAME") String userName,
        @Column(name = "PASS") String pass,
        @Column(name = "DESCRIPTION") String description,
        @Column(name = "CONNECTION_NAME") String connectionName,
        @Column(name = "DRIVER_NAME") String driverName,
        @Column(name = "ID_DB") Long idDb,
        @Column(name = "PORT") String port,
        @Column(name = "TYPE_DB") String typeDb,
        @Column(name = "INSERT_DATE") Timestamp insertDate
) {}