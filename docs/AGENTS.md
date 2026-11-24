# AGENTS.md — Quy tắc phát triển cho AI Coding Agent

Tài liệu này cung cấp ngữ cảnh kiến trúc, chuẩn mực kỹ thuật và các rào cản an toàn cho AI Coding Agent (Cursor, Claude Code, GitHub Copilot...) khi hỗ trợ phát triển dự án **Fault-Tolerant & AI-Ready E-Commerce Data Platform**.

---

## 1. Tổng quan dự án & Tech Stack

* **Mục tiêu:** Xây dựng nền tảng dữ liệu thương mại điện tử chịu lỗi và sẵn sàng cho AI (ẩn danh PII, chuẩn hóa Semantic Metadata).
* **Phạm vi generator v1:** Sinh đúng năm tình huống kiểm thử gồm trùng phiên bản đơn hàng, khóa đơn hoặc khách hàng bị NULL, giá trị đơn âm, đơn đến muộn và PII trong văn bản đánh giá. Sai định dạng và thay đổi schema (schema drift) là hướng mở rộng sau v1.
* **Tech Stack cốt lõi:**
  * **Ngôn ngữ:** Python 3.11+, SQL (DuckDB dialect).
  * **Lưu trữ thô (Data Lake):** MinIO (S3-compatible), định dạng Parquet phân vùng theo ngày (`dt=YYYY-MM-DD`).
  * **Biến đổi & Kiểm thử:** dbt Core + DuckDB.
  * **Điều phối:** Apache Airflow (hoặc Dagster) chạy qua Docker Compose.
  * **Chất lượng code:** `ruff` (Python), `sqlfluff` (SQL), `pytest`, dbt tests.

---

## 2. Kiến trúc dữ liệu (Medallion + Quarantine Pattern)

Mọi luồng biến đổi dữ liệu phải tuân thủ nghiêm ngặt thứ tự sau:

1. **Bronze (`models/bronze/`):** Đọc nguyên bản từ file Parquet trên MinIO. Không lọc bỏ dòng lỗi ở tầng này. Chỉ đổi tên cột chuẩn `snake_case` và ép kiểu dữ liệu an toàn (`TRY_CAST`).
2. **Silver (`models/silver/`):**
   * Khử trùng lặp bằng `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC)`.
   * Chuẩn hóa toàn bộ timestamp về `UTC`.
   * **Luồng Quarantine (Bắt buộc):** Không bao giờ âm thầm xóa (`WHERE ...`) các bản ghi hỏng (`order_id IS NULL`, `amount < 0`, `customer_id IS NULL`). Phải tách chúng sang bảng `quarantine_orders` kèm cột `error_reason` và `quarantined_at`.
   * Ẩn danh toàn bộ thông tin cá nhân nhạy cảm (PII: email, số điện thoại) trong cột văn bản trước khi lưu ở tầng Silver.
3. **Gold (`models/gold/`):**
   * Mô hình hóa theo **Star Schema** (`fct_orders`, `dim_customers`, `dim_products`).
   * Sử dụng **dbt Snapshots** (SCD Type 2) cho các chiều có thay đổi lịch sử (ví dụ: hạng thành viên khách hàng).
   * Mọi chỉ số nghiệp vụ quan trọng (`GMV`, `Net Revenue`, `Order Count`) phải được định nghĩa rõ ràng trong `_gold.yml`.

---

## 3. Quy tắc viết code bắt buộc

### Quy tắc SQL & dbt
* **Cấm `SELECT *`** ở tầng Silver và Gold. Luôn liệt kê rõ tên cột cần thiết để tối ưu hóa lưu trữ theo cột (columnar storage).
* **Cấu trúc CTE mô-đun:** Chia query thành các CTE rõ nghĩa theo trình tự: `source` → `filtered` → `deduplicated` → `transformed` → `final`. Không dùng subquery lồng nhau nhiều tầng.
* **Tính Idempotent & Incremental:** Các bảng xử lý dữ liệu lớn phải dùng `materialized='incremental'` với chiến lược `merge` và cửa sổ quét lùi 3 ngày (`3-day lookback window`) để xử lý dữ liệu đến muộn (late-arriving data) mà không tạo bản ghi trùng lặp khi chạy lại.
* **Lọc sớm & Tránh bọc hàm lên cột lọc:** Viết `WHERE created_at >= '2026-01-01'` thay vì `WHERE YEAR(created_at) = 2026` để tận dụng partition pruning.
* **Tài liệu & Kiểm thử đi kèm:** Mọi model mới tạo ở Silver/Gold **bắt buộc** phải có khai báo tương ứng trong file `.yml` kèm mô tả từng cột và tối thiểu các test `unique`, `not_null` cho khóa chính.

### Quy tắc Python
* Sử dụng **Type Hints** cho tất cả hàm và tham số.
* Dùng thư viện `logging` có cấu trúc thay vì `print()`.
* Mọi script sinh dữ liệu giả lập trong `data_generator/` phải hỗ trợ `random_seed` cố định để kết quả có thể tái lập (deterministic) khi chạy test.
* Đọc toàn bộ thông tin kết nối/secret từ biến môi trường (`.env`), tuyệt đối không hardcode vào code.

---

## 4. Quy trình kiểm chứng (Verification Commands)

Trước khi kết thúc bất kỳ thay đổi nào, Agent cần hướng dẫn hoặc thực thi các lệnh kiểm chứng tương ứng trong `Makefile`:

```bash
# Kiểm tra format code Python và SQL
make lint          # Chạy ruff check & sqlfluff lint

# Chạy unit test Python (data generator, PII masking)
make test-py       # Chạy pytest tests/

# Chạy biến đổi dữ liệu và toàn bộ bài test chất lượng (bao gồm đối soát doanh thu)
make dbt-build     # Chạy dbt build --profiles-dir dbt_project/

# Chạy đo đạc hiệu năng (khi chỉnh sửa query tối ưu)
make benchmark     # Chạy python benchmarks/run_benchmark.py
```

### Quy tắc Đối soát vàng (Reconciliation Invariant)
Bất kỳ thay đổi nào liên quan đến bảng đơn hàng đều phải vượt qua custom test `assert_revenue_reconciliation.sql`:
> **Tổng giá trị đơn hàng ở Gold (`fct_orders`) + Tổng giá trị đơn hàng ở Quarantine (`quarantine_orders`) PHẢI BẰNG Tổng giá trị đơn hàng ở Bronze sau khi khử trùng lặp.**

---

## 5. Rào cản an toàn (Guardrails — Agent KHÔNG ĐƯỢC làm)

1. **Không sửa đổi hoặc ghi đè dữ liệu thô** trong MinIO/Bronze (giữ nguyên tính bất biến của Data Lake).
2. **Không xóa, comment out hoặc hạ cấp độ (từ `error` xuống `warn`) các bài test dữ liệu** chỉ để làm CI/CD vượt qua. Phải sửa nguyên nhân gốc rễ trong logic biến đổi.
3. **Không tự ý thêm công nghệ nặng** (như Apache Spark, Kafka, Kubernetes) vào dự án khi quy mô dữ liệu giả lập (< 100GB) vẫn xử lý tối ưu bằng DuckDB.
4. **Không tự suy diễn định nghĩa nghiệp vụ:** Nếu có sự mơ hồ về công thức tính (ví dụ: `Net Revenue` có trừ phí vận chuyển hay đơn hoàn không), hãy hỏi lại người dùng trước khi viết code.

---

## 6. Phong cách cộng tác mong muốn

* Khi đề xuất giải pháp thiết kế hoặc tối ưu SQL, luôn giải thích ngắn gọn **sự đánh đổi (trade-off)** về tốc độ, độ phức tạp và khả năng bảo trì.
* Khi tối ưu hóa truy vấn, luôn ưu tiên đối chiếu bằng `EXPLAIN ANALYZE` thay vì tối ưu dựa trên phỏng đoán.
