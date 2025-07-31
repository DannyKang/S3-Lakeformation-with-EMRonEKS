# S3 Tables to S3 Iceberg Migration Summary

## 변경 사항 요약

이 프로젝트는 S3 Tables에서 S3 Iceberg 형식으로 변경되었습니다.

## 주요 변경 사항

### 1. 새로 생성된 스크립트

#### `01-create-s3-bucket.sh` (기존: `01-create-s3-table-bucket.sh`)
- **변경점**: S3 Tables 대신 일반 S3 버킷 + Glue Catalog 사용
- **새로운 기능**:
  - Glue 데이터베이스 생성
  - Iceberg 테이블 위치 설정
  - CREATE TABLE 쿼리 참고 파일 생성

#### `02-load-data-to-iceberg.sh` (기존: `02-load-data-to-s3-tables.sh`)
- **변경점**: Iceberg 테이블 생성 및 데이터 적재
- **새로운 기능**:
  - Athena를 통한 Iceberg 테이블 생성
  - CSV 외부 테이블을 통한 데이터 적재
  - INSERT INTO를 사용한 Iceberg 테이블 데이터 삽입

#### `04-setup-lakeformation-permissions-iceberg.sh` (기존: `04-setup-lakeformation-permissions-native.sh`)
- **변경점**: Glue Catalog 기반 Lake Formation 권한 설정
- **새로운 기능**:
  - Glue 데이터베이스/테이블 기반 권한 설정
  - S3 리소스 등록 및 데이터 위치 권한
  - Iceberg 형식에 최적화된 FGAC 설정

### 2. 수정된 스크립트

#### `03-create-iam-roles.sh`
- **변경점**: S3 Tables 권한을 S3 + Athena 권한으로 변경
- **수정 사항**:
  - `s3tables:*` 권한 제거
  - `s3:GetObject`, `s3:ListBucket` 권한 추가
  - `athena:*` 권한 추가

#### `05-setup-emr-on-eks.sh`
- **변경점**: 환경 변수명 변경
- **수정 사항**:
  - `TABLE_BUCKET_NAME` → `ICEBERG_BUCKET_NAME`
  - 스크립트 참조 경로 업데이트

#### `README.md`
- **변경점**: 전체 문서를 Iceberg 형식에 맞게 업데이트
- **주요 수정 사항**:
  - 제목: "S3 Tables" → "S3 Iceberg"
  - 아키텍처 다이어그램 업데이트
  - 단계별 실행 가이드 수정 (8단계 → 7단계)
  - 기술 스택 정보 업데이트
  - 프로젝트 구조 업데이트

## 환경 변수 변경 사항

### 기존 (.env)
```bash
TABLE_BUCKET_NAME=seoul-bike-demo-{ACCOUNT_ID}-{TIMESTAMP}
BUCKET_ARN=arn:aws:s3tables:...
NAMESPACE=bike_db
S3_TABLES_CATALOG=s3tablescatalog/...
```

### 새로운 (.env)
```bash
ICEBERG_BUCKET_NAME=seoul-bike-iceberg-{ACCOUNT_ID}-{TIMESTAMP}
DATABASE_NAME=bike_db
TABLE_NAME=bike_rental_data
ICEBERG_TABLE_LOCATION=s3://버킷명/data/bike_db/bike_rental_data/
```

## 실행 순서 변경

### 기존 (8단계)
1. `01-create-s3-table-bucket.sh`
2. `02-load-data-to-s3-tables.sh`
3. `03-create-iam-roles.sh`
4. `04-setup-lakeformation-permissions-native.sh`
5. `05-setup-emr-on-eks.sh`
6. `06-setup-lake-formation-fgac.sh`
7. `07-run-emr-jobs.sh`
8. `08-verify-and-analyze.sh`

### 새로운 (7단계)
1. `01-create-s3-bucket.sh`
2. `02-load-data-to-iceberg.sh`
3. `03-create-iam-roles.sh`
4. `04-setup-lakeformation-permissions-iceberg.sh`
5. `05-setup-emr-on-eks.sh`
6. `06-run-emr-jobs.sh`
7. `07-verify-and-analyze.sh`

## 기술적 장점

### S3 Iceberg 형식의 장점
1. **스키마 진화**: 테이블 스키마를 쉽게 변경 가능
2. **시간 여행**: 과거 버전의 데이터 조회 가능
3. **성능 최적화**: 파티셔닝 및 압축 최적화
4. **호환성**: 다양한 엔진(Spark, Athena, Trino 등)에서 사용 가능
5. **비용 효율성**: S3 Tables 대비 저렴한 스토리지 비용

### Lake Formation 호환성
- Glue Catalog 기반으로 완전한 FGAC 지원
- Row-level, Column-level, Cell-level 보안 모두 지원
- EMR on EKS와의 완벽한 통합

## 마이그레이션 가이드

기존 S3 Tables 환경에서 S3 Iceberg로 마이그레이션하려면:

1. 기존 환경 정리
2. 새로운 스크립트 순서대로 실행
3. 데이터 검증 및 테스트
4. 기존 리소스 정리

## 주의사항

- 기존 `.env` 파일은 새로운 형식과 호환되지 않음
- IAM 권한이 변경되어 기존 역할 업데이트 필요
- Spark 코드는 Iceberg 형식에 맞게 수정 필요 (별도 작업)

## 다음 단계

1. Spark 분석 코드를 Iceberg 형식에 맞게 수정
2. EMR Job 템플릿 업데이트
3. 결과 검증 스크립트 작성
4. 성능 테스트 및 최적화