package com.viettel.sync.core;

import com.viettel.sync.core.Annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;

public class GenericRepository<T> {
    private static final Logger logger = LoggerFactory.getLogger(GenericRepository.class);
    private static final int BATCH_SIZE = 1000;

    /**
     * Xóa theo danh sách ID
     * @param schema: Tên schema (VD: DATALAKE_CONFIG_TD)
     */
    public void deleteByIds(Connection conn, String schema, Class<T> clazz, List<Long> idsToDelete) {
        if (idsToDelete == null || idsToDelete.isEmpty()) return;
        if (!clazz.isAnnotationPresent(Table.class)) return;

        // Lấy tên bảng từ Annotation và gắn Schema vào
        String rawTableName = clazz.getAnnotation(Table.class).name();
        String fullTableName = schema + "." + rawTableName;

        // Tìm cột ID
        String idCol = "ID";
        for (Field f : clazz.getDeclaredFields()) {
            if (f.isAnnotationPresent(Id.class)) {
                Column colAnn = f.getAnnotation(Column.class);
                if (colAnn != null) {
                    idCol = colAnn.name();
                }
                break;
            }
        }

        String sql = "DELETE FROM " + fullTableName + " WHERE " + idCol + " = ?";
        logger.debug("Executing Delete: {}", sql);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            for (Long id : idsToDelete) {
                ps.setLong(1, id);
                ps.addBatch();
                if (++count % BATCH_SIZE == 0) ps.executeBatch();
            }
            ps.executeBatch();
            logger.info("   -> [DELETE] Đã xóa {} dòng rác khỏi bảng {}", idsToDelete.size(), fullTableName);
        } catch (Exception e) {
            logger.error("❌ Lỗi Delete bảng " + fullTableName, e);
        }
    }

    /**
     * Xóa sạch bảng (Truncate)
     */
    public void truncate(Connection conn, String schema, Class<T> clazz) {
        if (!clazz.isAnnotationPresent(Table.class)) return;

        String rawTableName = clazz.getAnnotation(Table.class).name();
        String fullTableName = schema + "." + rawTableName;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE " + fullTableName);
            logger.info("   -> [TRUNCATE] Đã làm sạch bảng {}", fullTableName);
        } catch (Exception e) {
            logger.error("❌ Lỗi Truncate " + fullTableName, e);
        }
    }

    /**
     * Insert danh sách
     */
    public void insertBatch(Connection conn, String schema, List<T> items) {
        if (items == null || items.isEmpty()) return;

        Class<?> clazz = items.get(0).getClass();
        if (!clazz.isAnnotationPresent(Table.class)) return;

        String rawTableName = clazz.getAnnotation(Table.class).name();
        String fullTableName = schema + "." + rawTableName;

        Field[] fields = clazz.getDeclaredFields();
        StringJoiner cols = new StringJoiner(",");
        StringJoiner params = new StringJoiner(",");

        // Xây dựng câu SQL động
        for (Field f : fields) {
            if (f.isAnnotationPresent(Column.class)) {
                cols.add(f.getAnnotation(Column.class).name());
                params.add("?");
                f.setAccessible(true);
            }
        }

        String sql = "INSERT INTO " + fullTableName + " (" + cols + ") VALUES (" + params + ")";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = 0;
            for (T item : items) {
                int idx = 1;
                for (Field f : fields) {
                    if (f.isAnnotationPresent(Column.class)) {
                        ps.setObject(idx++, f.get(item));
                    }
                }
                ps.addBatch();
                if (++count % BATCH_SIZE == 0) ps.executeBatch();
            }
            ps.executeBatch();
            logger.info("   -> [INSERT] Đã chèn {} dòng vào bảng {}", items.size(), fullTableName);
        } catch (Exception e) {
            logger.error("❌ Lỗi Insert bảng " + fullTableName, e);
            // Ném lỗi ra ngoài để Service biết mà Rollback nếu cần
            throw new RuntimeException(e);
        }
    }

    /**
     * Cập nhật Connection Name & Driver Name (Dành riêng cho bảng Connection)
     * Đã sửa lỗi Hardcode tên bảng và thiếu schema.
     */
    public void updateConnectionInfo(Connection conn, String schema, Class<T> clazz, Long id, String newName, String newDriver) {
        if (!clazz.isAnnotationPresent(Table.class)) return;

        String rawTableName = clazz.getAnnotation(Table.class).name();
        String fullTableName = schema + "." + rawTableName;

        // SQL động: update schema.table set ...
        String sql = "UPDATE " + fullTableName + " SET CONNECTION_NAME = ?, DRIVER_NAME = ? WHERE ID = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newName);
            ps.setString(2, newDriver);
            ps.setLong(3, id);
            ps.executeUpdate();
            // logger.debug("Updated ID {} -> Name: {}", id, newName);
        } catch (Exception e) {
            logger.error("❌ Lỗi Update Info cho ID " + id + " bảng " + fullTableName, e);
        }
    }
}