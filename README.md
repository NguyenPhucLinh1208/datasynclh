# Lệnh cài vào repo
mvn install:install-file -Dfile="D:\linhnp20\Project\JavaScalaCode\syncToo\libs\ojdbc8-12.2.0.1.jar" -DgroupId=com.viettel.local -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar

mvn install:install-file -Dfile=./libs/PassCodeLake.jar -DgroupId=com.viettel.local -DartifactId=PassCodeLake -Dversion=1.0 -Dpackaging=jar

mvn install:install-file -Dfile=./libs/PassCodeLakeHouse.jar -DgroupId=com.viettel.local -DartifactId=PassCodeLakeHouse -Dversion=1.0 -Dpackaging=jar

# Tổ chức cây thư mục
- **`/`**
    - **`pom.xml`**
    - **`README.md`**
    - **`src/main/java/com/viettel/sync`**
        - **`app`** — entrypoint, CLI hoặc scheduler (class `Main` hoặc `SyncJob`)
        - **`config`** — cấu hình DataSource, đọc properties, factory cho 2 DataSource (source/target)
        - **`model`** — POJO/DTO cho 5 bảng (mỗi bảng 1 class)
        - **`repository`** — DAO dùng JDBC/JDBCTemplate; tách `source` và `target` repository nếu cần
        - **`service`** — orchestration: reader → transformer → writer, retry, idempotency
        - **`transform`** — các adapter cho biến đổi (path normalize, decode/encode)
        - **`util`** — helper chung: mapping, validation, checksum, retry util, constants
    - **`src/main/resources`**
        - **`application.properties`** hoặc `application.yaml` — cấu hình DB, batch size, log level (support override bằng env)
        - **`logback.xml`** — cấu hình logging
    - **`src/test/java`** — unit test cho `service` và `transform`, integration test cho `repository`
    - **`scripts`** — shell scripts để chạy jar, migrate, hoặc cron wrapper (nếu cần)
    - **`ops`** — hướng dẫn deploy, sample systemd unit, hoặc kịch bản chạy trên server

---

### Vai trò chi tiết từng thư mục
- **`app`**
    - **Vai trò**: điểm khởi chạy; parse args, load config, khởi tạo 2 DataSource, schedule job.
    - **Quan trọng**: giữ *ít logic*, chỉ orchestration và lifecycle.

- **`config`**
    - **Vai trò**: tách cấu hình ra khỏi code; tạo bean DataSource cho schema `DATA_LAKE_CONFIG` và `DATA_LAKE_CONFIG_TD`.
    - **Quan trọng**: hỗ trợ override bằng biến môi trường để deploy an toàn.

- **`model`**
    - **Vai trò**: định nghĩa cấu trúc 5 bảng; thêm annotation nếu dùng mapper; giữ mapping 1:1 với cột DB.

- **`repository`**
    - **Vai trò**: đọc/ghi DB; implement batch read (limit/offset hoặc cursor), upsert/merge vào target.
    - **Quan trọng**: tách rõ source/target để dễ mock test.

- **`service`** và **`transform`**
    - **Vai trò**: *service* điều phối; *transform* chứa logic biến đổi (chuẩn hóa đường dẫn, giải mã bằng jar A, mã hóa bằng jar B).
    - **Quan trọng**: viết interface cho encoder/decoder để dễ thay implementation và unit test.

- **`util`**
    - **Vai trò**: helper chung (logging helpers, retry/backoff, checksum để idempotency).
    - **Quan trọng**: không chứa business logic.

- **`resources`**
    - **Vai trò**: cấu hình, secrets (chỉ tham chiếu; không commit mật khẩu), logback.
    - **Quan trọng**: dùng vault/secret manager cho production.

### Chú ý dùng maven shade plugin để đóng gói thành fat jar.