package com.viettel.sync.app;

import com.viettel.sync.config.AppConfigLoader;
import com.viettel.sync.service.SyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        logger.info("==========================================");
        logger.info("       BẮT ĐẦU TOOL ĐỒNG BỘ DỮ LIỆU       ");
        logger.info("==========================================");

        try {

            // Log kiểm tra lại schema (Dùng cách gọi Getter chuẩn của bạn)
            logger.info("Schema Source: {}", AppConfigLoader.getDbConfig().sourceSchema());
            logger.info("Schema Target: {}", AppConfigLoader.getDbConfig().targetSchema());

            // 2. Khởi tạo Service chính
            // (Service sẽ tự khởi tạo các Repository bên trong constructor của nó)
            SyncService syncService = new SyncService();

            // 3. Chạy luồng đồng bộ
            syncService.runSync();

        } catch (Exception e) {
            logger.error("❌ CHƯƠNG TRÌNH GẶP LỖI KHÔNG MONG MUỐN:", e);
        } finally {
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            logger.info("==========================================");
            logger.info("          KẾT THÚC CHƯƠNG TRÌNH           ");
            logger.info("       Tổng thời gian: {} ms ({} giây)", duration, duration / 1000.0);
            logger.info("==========================================");
        }
    }
}
