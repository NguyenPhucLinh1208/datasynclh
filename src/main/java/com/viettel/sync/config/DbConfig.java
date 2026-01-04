package com.viettel.sync.config;

public record DbConfig(
        String url,
        String username,
        String password,
        String sourceSchema,
        String targetSchema
) {}