#!/bin/bash

# S3 Iceberg 버킷 생성 스크립트
# 고유한 버킷명 생성으로 다중 사용자 환경 지원

set -e

REGION="ap-northeast-2"
DATABASE_NAME="bike_db"
TABLE_NAME="bike_rental_data"

# 계정 ID 확인
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 기존 .env 파일이 있으면 기존 버킷 재사용
if [ -f ".env" ] && grep -q "ICEBERG_BUCKET_NAME=" .env; then
    echo "기존 .env 파일 발견. 기존 버킷 정보를 로드합니다..."
    source .env
    echo "기존 버킷 재사용: $ICEBERG_BUCKET_NAME"
    REUSE_EXISTING=true
else
    # 새로운 버킷명 생성
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    ICEBERG_BUCKET_NAME="seoul-bike-iceberg-${ACCOUNT_ID}-${TIMESTAMP}"
    REUSE_EXISTING=false
fi

echo "=== S3 Iceberg 버킷 생성 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "버킷명: $ICEBERG_BUCKET_NAME"
echo "데이터베이스: $DATABASE_NAME"
echo "테이블명: $TABLE_NAME"
echo ""

# 1. S3 버킷 생성
if [ "$REUSE_EXISTING" = false ]; then
    echo "1. S3 버킷 생성 중..."
    aws s3 mb s3://$ICEBERG_BUCKET_NAME --region $REGION
    echo "✅ S3 버킷 생성 완료: $ICEBERG_BUCKET_NAME"
else
    echo "1. 기존 S3 버킷 재사용: $ICEBERG_BUCKET_NAME"
fi

# 2. Glue 데이터베이스 생성
echo ""
echo "2. Glue 데이터베이스 생성 중..."

# 기존 데이터베이스 확인
if aws glue get-database --name $DATABASE_NAME --region $REGION >/dev/null 2>&1; then
    echo "⚠️  기존 데이터베이스 발견: $DATABASE_NAME"
else
    aws glue create-database \
        --database-input Name=$DATABASE_NAME,Description="Seoul bike rental data for Lake Formation FGAC demo" \
        --region $REGION
    echo "✅ Glue 데이터베이스 생성 완료: $DATABASE_NAME"
fi

# 3. Athena 결과 버킷 확인/생성
echo ""
echo "3. Athena 결과 버킷 확인/생성 중..."
ATHENA_RESULTS_BUCKET="aws-athena-query-results-${ACCOUNT_ID}-${REGION}"
if ! aws s3 ls "s3://${ATHENA_RESULTS_BUCKET}" >/dev/null 2>&1; then
    echo "Athena 결과 버킷 생성 중: ${ATHENA_RESULTS_BUCKET}"
    aws s3 mb "s3://${ATHENA_RESULTS_BUCKET}" --region $REGION
fi
echo "✅ Athena 결과 버킷 준비 완료: $ATHENA_RESULTS_BUCKET"

# 4. 환경 설정 파일 생성
echo ""
echo "4. 환경 설정 파일 생성 중..."

# 생성 정보 변수 설정
CREATED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
CREATED_BY="$(aws sts get-caller-identity --query Arn --output text)"

# Iceberg 테이블 위치 설정
ICEBERG_TABLE_LOCATION="s3://${ICEBERG_BUCKET_NAME}/data/${DATABASE_NAME}/${TABLE_NAME}/"

cat > .env << EOF
# AWS 환경 설정 (자동 생성됨 - 수정하지 마세요)
ACCOUNT_ID=$ACCOUNT_ID
REGION=$REGION

# S3 Iceberg 설정 (자동 생성됨 - 수정하지 마세요)
ICEBERG_BUCKET_NAME=$ICEBERG_BUCKET_NAME
DATABASE_NAME=$DATABASE_NAME
TABLE_NAME=$TABLE_NAME
ICEBERG_TABLE_LOCATION=$ICEBERG_TABLE_LOCATION

# 테이블 스키마 정보
TABLE_COLUMNS="rental_id,station_id,station_name,rental_date,return_date,usage_min,distance_meter,birth_year,gender,user_type,district"
TABLE_SCHEMA_VERSION="v1_iceberg_format"
TABLE_CREATION_METHOD="GLUE_CATALOG"

# Athena 설정
ATHENA_RESULTS_BUCKET=$ATHENA_RESULTS_BUCKET

# 생성 정보
CREATED_AT="$CREATED_AT"
CREATED_BY="$CREATED_BY"
EOF

echo "✅ 환경 설정 파일 생성 완료: .env"

# 5. CREATE TABLE 쿼리 생성 (참고용)
echo ""
echo "5. CREATE TABLE 쿼리 생성 중..."

cat > create_iceberg_table.sql << EOF
-- Iceberg 테이블 생성 쿼리 (Athena에서 실행)
CREATE TABLE ${DATABASE_NAME}.${TABLE_NAME} (
    rental_id string,
    station_id string,
    station_name string,
    rental_date string,
    return_date string,
    usage_min int,
    distance_meter double,
    birth_year string,
    gender string,
    user_type string,
    district string
)
USING ICEBERG
LOCATION '${ICEBERG_TABLE_LOCATION}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- 테이블 확인 쿼리
SHOW TABLES IN ${DATABASE_NAME};
DESCRIBE ${DATABASE_NAME}.${TABLE_NAME};
SELECT COUNT(*) FROM ${DATABASE_NAME}.${TABLE_NAME};
EOF

echo "✅ CREATE TABLE 쿼리 파일 생성 완료: create_iceberg_table.sql"

# 6. 설정 요약 출력
echo ""
echo "=== S3 Iceberg 버킷 및 환경 설정 완료 ==="
echo ""
echo "📋 생성된 리소스 요약:"
echo "┌─────────────────────────┬─────────────────────────────────────┐"
echo "│ 리소스                  │ 값                                  │"
echo "├─────────────────────────┼─────────────────────────────────────┤"
echo "│ S3 버킷                 │ $ICEBERG_BUCKET_NAME                │"
echo "│ Glue 데이터베이스       │ $DATABASE_NAME                      │"
echo "│ 테이블명                │ $TABLE_NAME                         │"
echo "│ Iceberg 위치            │ $ICEBERG_TABLE_LOCATION             │"
echo "│ 리전                    │ $REGION                             │"
echo "│ Athena 결과 버킷        │ $ATHENA_RESULTS_BUCKET              │"
echo "└─────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "📊 테이블 스키마 정보:"
echo "   • rental_id (STRING) - 대여 ID"
echo "   • station_id (STRING) - 정거장 ID"
echo "   • station_name (STRING) - 정거장명"
echo "   • rental_date (STRING) - 대여 일시"
echo "   • return_date (STRING) - 반납 일시"
echo "   • usage_min (INT) - 사용 시간(분)"
echo "   • distance_meter (DOUBLE) - 이동 거리(미터)"
echo "   • birth_year (STRING) - 출생년도"
echo "   • gender (STRING) - 성별"
echo "   • user_type (STRING) - 사용자 유형"
echo "   • district (STRING) - 구"
echo ""
echo "🔧 환경 설정:"
echo "   • 설정 파일: .env"
echo "   • CREATE TABLE 쿼리: create_iceberg_table.sql"
echo "   • 다른 스크립트들이 이 설정을 자동으로 사용합니다"
echo ""
echo "📋 **Apache Iceberg 형식**:"
echo "   • 버킷: ${ICEBERG_BUCKET_NAME}"
echo "   • 데이터베이스: ${DATABASE_NAME}"
echo "   • 테이블: ${TABLE_NAME}"
echo "   • 형식: Apache Iceberg (Parquet + Snappy 압축)"
echo "   • 위치: ${ICEBERG_TABLE_LOCATION}"
echo ""
echo "⚠️  중요사항:"
echo "   • .env 파일을 삭제하지 마세요"
echo "   • 버킷명은 계정별로 고유하게 생성됩니다"
echo "   • 테이블은 다음 단계에서 생성됩니다"
echo "   • Iceberg 형식으로 Lake Formation FGAC 지원"
echo ""
echo "✅ 다음 단계: ./scripts/02-load-data-to-iceberg.sh"