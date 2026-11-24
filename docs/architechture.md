# Cấu trúc thư mục dự án: Fault-Tolerant & AI-Ready E-Commerce Data Platform

Cấu trúc dưới đây được thiết kế theo tư duy **Software Engineering chuẩn production**, tách biệt rõ ràng giữa tầng sinh dữ liệu giả lập, tầng điều phối, tầng biến đổi dữ liệu (dbt), tầng benchmark tối ưu và tầng phục vụ AI.

```text
ecommerce-ai-ready-platform/
├── .github/
│   └── workflows/
│       └── ci.yml                       # Tự động chạy sqlfluff, pytest và dbt test trên mẫu dữ liệu nhỏ khi tạo PR
├── ai_serving/
│   ├── semantic_context.py              # Đọc metadata từ schema.yml của dbt để nạp ngữ cảnh cho LLM
│   ├── text_to_sql_demo.py              # Demo hỏi đáp ngôn ngữ tự nhiên truy vấn trực tiếp vào tầng Gold
│   └── eval_questions.json              # Bộ câu hỏi nghiệp vụ mẫu dùng để kiểm thử độ chính xác của AI
├── benchmarks/
│   ├── naive_query.sql                  # Phiên bản truy vấn chưa tối ưu (full scan, SELECT *, lọc bằng hàm)
│   ├── optimized_query.sql              # Phiên bản đã tối ưu (partition pruning, incremental, lọc trước JOIN)
│   ├── run_benchmark.py                 # Script chạy EXPLAIN ANALYZE và đo thời gian/RAM giữa 2 phiên bản
│   └── results.md                       # Ghi lại bảng số liệu so sánh & ảnh chụp Execution Plan
├── data_generator/
│   ├── config.py                        # Cấu hình tỷ lệ tiêm lỗi (trùng lặp, null, giá âm, dữ liệu muộn, PII)
│   ├── chaos_generator.py               # Script Python sinh 5–10 triệu dòng đơn hàng, user, review dạng Parquet
│   └── upload_to_minio.py               # Đẩy file Parquet theo partition ngày (dt=YYYY-MM-DD) lên MinIO
├── dbt_project/
│   ├── dbt_project.yml                  # Cấu hình chung của dbt (materialization, thư mục models)
│   ├── profiles.yml.example             # Mẫu kết nối DuckDB / MinIO (không commit credential thật)
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── _sources.yml             # Khai báo nguồn Parquet thô từ MinIO + kiểm tra freshness
│   │   │   └── stg_raw_orders.sql       # Đọc dữ liệu thô, chuẩn hóa tên cột & ép kiểu cơ bản
│   │   ├── silver/
│   │   │   ├── _silver.yml              # Tài liệu mô tả và bộ test (unique, not_null, accepted_values)
│   │   │   ├── int_orders_deduped.sql   # Khử trùng lặp bằng ROW_NUMBER(), chuẩn hóa múi giờ UTC
│   │   │   ├── int_orders_clean.sql     # Dữ liệu đơn hàng sạch (incremental merge 3 ngày)
│   │   │   ├── quarantine_orders.sql    # Cô lập dòng lỗi (null ID, giá âm) kèm cột error_reason
│   │   │   └── int_reviews_masked.sql   # Làm sạch text và ẩn danh PII (SĐT, email) trong đánh giá
│   │   └── gold/
│   │       ├── _gold.yml                # Semantic metadata chi tiết (định nghĩa GMV, Net Revenue...) cho AI
│   │       ├── fct_orders.sql           # Bảng Fact đơn hàng tối ưu cho phân tích
│   │       ├── dim_customers.sql        # Bảng Dimension khách hàng (kết nối SCD Type 2 snapshot)
│   │       ├── dim_products.sql         # Bảng Dimension sản phẩm
│   │       └── mart_daily_revenue.sql   # Bảng tổng hợp sẵn (pre-aggregated) giảm độ trễ cho BI & AI Agent
│   ├── snapshots/
│   │   └── snp_customers.sql            # Lưu lịch sử thay đổi hạng thành viên khách hàng (SCD Type 2)
│   ├── macros/
│   │   ├── mask_pii.sql                 # Macro ẩn danh thông tin nhạy cảm bằng Regex
│   │   └── generate_schema_name.sql     # Quy tắc đặt tên schema tách biệt môi trường dev/prod
│   └── tests/
│       └── assert_revenue_reconciliation.sql  # Custom test đối soát: Gold + Quarantine = Bronze (sau khử trùng)
├── orchestration/
│   └── dags/
│       └── ecommerce_pipeline_dag.py    # Airflow/Dagster pipeline: Ingestion -> dbt build -> Reconciliation -> Alert
├── tests/
│   ├── test_chaos_generator.py          # Unit test kiểm tra script sinh dữ liệu chạy đúng tỷ lệ lỗi & deterministic
│   └── test_pii_masking.py              # Unit test đảm bảo không lọt email/SĐT sau khi làm sạch
├── .env.example                         # Mẫu biến môi trường (MinIO keys, LLM API key nếu dùng)
├── .gitignore                           # Bỏ qua dữ liệu local, file .duckdb, .env, logs
├── .sqlfluff                            # Quy tắc format SQL tự động cho toàn dự án
├── agent.md                             # Bộ quy tắc & ngữ cảnh hướng dẫn AI Coding Agent làm việc đúng chuẩn
├── docker-compose.yml                   # Dựng MinIO, Airflow/Dagster và môi trường chạy chỉ với 1 lệnh
├── Makefile                             # Tập hợp lệnh tắt: make up, make generate, make build, make test, make lint
├── pyproject.toml                       # Quản lý thư viện Python và cấu hình pytest/ruff
└── README.md                            # Tài liệu trình bày kiến trúc, trade-offs và kết quả benchmark
```

## Giải thích nhanh các thư mục trọng tâm

* **`data_generator/`**: Đóng vai trò "nguồn dữ liệu hỗn loạn" thay vì dùng file CSV sạch. Tách riêng `config.py` giúp bạn dễ dàng bật/tắt hoặc tăng giảm tỷ lệ từng loại lỗi khi demo cho nhà tuyển dụng.
* **`dbt_project/`**: Trái tim của hệ thống. Luồng dữ liệu đi rõ ràng từ `bronze` (nguyên bản) → `silver` (khử trùng, che PII và tách nhánh `quarantine_orders`) → `gold` (Star Schema và bảng tổng hợp sẵn).
* **`benchmarks/`**: Nơi chứng minh tư duy tối ưu hiệu năng bằng con số thực tế (`EXPLAIN ANALYZE`), thay vì chỉ nói lý thuyết suông trong CV.
* **`ai_serving/`**: Tầng ứng dụng nhẹ chứng minh dữ liệu tầng Gold đã thực sự **AI-Ready** nhờ metadata chuẩn hóa trong `_gold.yml` và dữ liệu văn bản đã loại bỏ PII.
* **`Makefile` & `docker-compose.yml`**: Đảm bảo tính tái lập — bất kỳ ai clone repo về đều có thể chạy thử toàn bộ hệ thống trong vài phút.