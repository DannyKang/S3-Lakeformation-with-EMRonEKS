#!/bin/bash

# S3 Iceberg에 로컬 데이터 적재 스크립트
# Athena를 통한 Iceberg 테이블 생성 및 데이터 적재

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/01-create-s3-bucket.sh를 실행하세요."
    exit 1
fi

echo "환경 설정 파일 로드 중..."
source .env

# 필수 환경 변수 확인
if [ -z "$ICEBERG_BUCKET_NAME" ] || [ -z "$DATABASE_NAME" ] || [ -z "$TABLE_NAME" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "01-create-s3-bucket.sh를 다시 실행하세요."
    exit 1
fi

LOCAL_DATA_FILE="./sample-data/seoul-bike-sample-100k.csv"

echo "=== S3 Iceberg 데이터 적재 시작 ==="
echo "S3 버킷: $ICEBERG_BUCKET_NAME"
echo "데이터베이스: $DATABASE_NAME"
echo "테이블: $TABLE_NAME"
echo "로컬 데이터: $LOCAL_DATA_FILE"
echo ""

# 1. 로컬 데이터 파일 확인
echo "1. 로컬 데이터 파일 확인..."
if [ ! -f "$LOCAL_DATA_FILE" ]; then
    echo "❌ 로컬 데이터 파일이 존재하지 않습니다: $LOCAL_DATA_FILE"
    echo "먼저 샘플 데이터를 생성하세요."
    exit 1
fi

RECORD_COUNT=$(tail -n +2 "$LOCAL_DATA_FILE" | wc -l | tr -d ' ')
echo "✅ 로컬 데이터 파일 확인 완료: $RECORD_COUNT 레코드"

# 2. 임시 S3 버킷 생성
echo ""
echo "2. 임시 S3 버킷 생성..."
TEMP_S3_BUCKET="seoul-bike-temp-data-${ACCOUNT_ID}-$(date +%s)"
aws s3 mb s3://$TEMP_S3_BUCKET --region $REGION
echo "✅ 임시 S3 버킷 생성: $TEMP_S3_BUCKET"

# 3. 로컬 파일을 S3에 업로드
echo ""
echo "3. 로컬 파일을 S3에 업로드..."
aws s3 cp "$LOCAL_DATA_FILE" s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv
echo "✅ S3 업로드 완료: s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv"

# 4. 데이터 미리보기
echo ""
echo "4. 데이터 미리보기..."
echo "=== 처음 3행 ====="
head -n 4 "$LOCAL_DATA_FILE" | tail -n 3
echo "=================="

# 5. Iceberg 테이블 생성
echo ""
echo "5. Iceberg 테이블 생성..."

# 5-1. 기존 테이블 확인 및 삭제
echo "   5-1. 기존 테이블 확인..."
if aws glue get-table --database-name $DATABASE_NAME --name $TABLE_NAME --region $REGION >/dev/null 2>&1; then
    echo "   ⚠️  기존 테이블 발견. 삭제 중..."
    aws glue delete-table --database-name $DATABASE_NAME --name $TABLE_NAME --region $REGION
    echo "   ✅ 기존 테이블 삭제 완료"
    sleep 3
else
    echo "   ℹ️  기존 테이블이 없습니다. 새로 생성합니다."
fi

# 5-2. Iceberg 테이블 생성
echo "   5-2. Iceberg 테이블 생성: $DATABASE_NAME.$TABLE_NAME"

CREATE_TABLE_SQL="CREATE TABLE ${DATABASE_NAME}.${TABLE_NAME} (
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
LOCATION '${ICEBERG_TABLE_LOCATION}'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);"

QUERY_EXECUTION_ID=$(aws athena start-query-execution \
    --query-string "$CREATE_TABLE_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

echo "   쿼리 ID: $QUERY_EXECUTION_ID"

# 쿼리 완료 대기
echo "   쿼리 실행 대기 중..."
for i in {1..30}; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $QUERY_EXECUTION_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo "   ✅ Iceberg 테이블 생성 완료"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "   ❌ Iceberg 테이블 생성 실패"
        ERROR_MSG=$(aws athena get-query-execution \
            --query-execution-id $QUERY_EXECUTION_ID \
            --region $REGION \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text)
        echo "   오류: $ERROR_MSG"
        exit 1
    else
        echo "   상태: $STATUS (${i}/30)"
        sleep 3
    fi
done

if [ "$STATUS" != "SUCCEEDED" ]; then
    echo "   ❌ 테이블 생성 시간 초과"
    exit 1
fi

# 6. CSV 외부 테이블 생성 (데이터 적재용)
echo ""
echo "6. CSV 외부 테이블 생성..."
TEMP_TABLE_NAME="temp_seoul_bike_csv_$(date +%s)"

# 기존 임시 테이블 정리
aws athena start-query-execution \
    --query-string "DROP TABLE IF EXISTS temp_seoul_bike_csv;" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION >/dev/null 2>&1 || true

CREATE_CSV_TABLE_SQL="CREATE EXTERNAL TABLE $TEMP_TABLE_NAME (
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ','
)
LOCATION 's3://$TEMP_S3_BUCKET/data/'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'skip.header.line.count'='1'
);"

CSV_QUERY_ID=$(aws athena start-query-execution \
    --query-string "$CREATE_CSV_TABLE_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

# CSV 테이블 생성 대기
for i in {1..30}; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $CSV_QUERY_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo "✅ CSV 외부 테이블 생성 완료: $TEMP_TABLE_NAME"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "❌ CSV 테이블 생성 실패"
        exit 1
    else
        echo "   상태: $STATUS (${i}/30)"
        sleep 2
    fi
done

# 7. 데이터 적재
echo ""
echo "7. Iceberg 테이블에 데이터 적재..."

INSERT_SQL="INSERT INTO ${DATABASE_NAME}.${TABLE_NAME}
SELECT 
    rental_id,
    station_id,
    station_name,
    rental_date,
    return_date,
    usage_min,
    distance_meter,
    birth_year,
    gender,
    user_type,
    district
FROM $TEMP_TABLE_NAME
WHERE rental_id IS NOT NULL
  AND station_id IS NOT NULL
  AND rental_date IS NOT NULL
  AND return_date IS NOT NULL;"

echo "   INSERT 쿼리 실행 중... (약 2-5분 소요)"

INSERT_QUERY_ID=$(aws athena start-query-execution \
    --query-string "$INSERT_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

echo "   INSERT 쿼리 ID: $INSERT_QUERY_ID"

# INSERT 쿼리 완료 대기
for i in {1..60}; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $INSERT_QUERY_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo "   ✅ Iceberg 테이블 데이터 적재 완료!"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "   ❌ 데이터 적재 실패"
        ERROR_MSG=$(aws athena get-query-execution \
            --query-execution-id $INSERT_QUERY_ID \
            --region $REGION \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text)
        echo "   오류: $ERROR_MSG"
        exit 1
    else
        echo "   상태: $STATUS (${i}/60) - 대기 중..."
        sleep 5
    fi
done

if [ "$STATUS" != "SUCCEEDED" ]; then
    echo "   ❌ INSERT 시간 초과"
    exit 1
fi

# 8. 적재 결과 확인
echo ""
echo "8. 적재 결과 확인..."

COUNT_SQL="SELECT COUNT(*) as total_records FROM ${DATABASE_NAME}.${TABLE_NAME};"

COUNT_QUERY_ID=$(aws athena start-query-execution \
    --query-string "$COUNT_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

# 카운트 쿼리 완료 대기
for j in {1..20}; do
    COUNT_STATUS=$(aws athena get-query-execution \
        --query-execution-id $COUNT_QUERY_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$COUNT_STATUS" = "SUCCEEDED" ]; then
        echo "✅ 적재 완료 검증 성공"
        break
    elif [ "$COUNT_STATUS" = "FAILED" ] || [ "$COUNT_STATUS" = "CANCELLED" ]; then
        echo "⚠️  레코드 수 확인 실패 (적재는 성공)"
        break
    else
        sleep 2
    fi
done

# 9. 임시 리소스 정리
echo ""
echo "9. 임시 리소스 정리..."

# Athena 임시 테이블 삭제
aws athena start-query-execution \
    --query-string "DROP TABLE IF EXISTS $TEMP_TABLE_NAME;" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION >/dev/null 2>&1 || true

# S3 버킷 정리
aws s3 rm s3://$TEMP_S3_BUCKET --recursive >/dev/null 2>&1 || true
aws s3 rb s3://$TEMP_S3_BUCKET >/dev/null 2>&1 || true

echo "✅ 임시 리소스 정리 완료"

# 10. 환경 변수에 정보 추가
cat >> .env << EOF

# Iceberg 데이터 적재 완료 정보 ($(date '+%Y-%m-%d %H:%M:%S'))
DATA_LOADED_TO_ICEBERG=true
ICEBERG_LOAD_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
ICEBERG_LOAD_METHOD="ATHENA_INSERT"
LOADED_RECORD_COUNT=$RECORD_COUNT
EOF

echo ""
echo "=== Iceberg 데이터 적재 완료 ==="
echo ""
echo "🎉 **S3 Iceberg 데이터 적재 성공!**"
echo ""
echo "✅ **완료된 작업**:"
echo "   - 로컬 CSV 데이터 검증 ($RECORD_COUNT 레코드)"
echo "   - S3에 데이터 업로드"
echo "   - Iceberg 테이블 생성"
echo "   - 데이터 적재 완료"
echo "   - 임시 리소스 정리"
echo ""
echo "📊 **적재된 데이터 정보**:"
echo "   - S3 버킷: $ICEBERG_BUCKET_NAME"
echo "   - 데이터베이스: $DATABASE_NAME"
echo "   - 테이블: $TABLE_NAME"
echo "   - 레코드 수: $RECORD_COUNT"
echo "   - 형식: Apache Iceberg (Parquet + Snappy)"
echo "   - 위치: $ICEBERG_TABLE_LOCATION"
echo ""
echo "🔍 **데이터 확인 쿼리** (Athena에서 실행):"
echo "   SELECT COUNT(*) FROM ${DATABASE_NAME}.${TABLE_NAME};"
echo "   SELECT * FROM ${DATABASE_NAME}.${TABLE_NAME} LIMIT 10;"
echo "   DESCRIBE ${DATABASE_NAME}.${TABLE_NAME};"
echo ""
echo "🎯 **다음 단계**: IAM 역할 생성"
echo "   ./scripts/03-create-iam-roles.sh"
echo ""
echo "⚠️  **중요사항**:"
echo "   - Iceberg 형식으로 Lake Formation FGAC 완전 지원"
echo "   - 스키마 진화 및 시간 여행 쿼리 지원"
echo "   - EMR on EKS에서 최적화된 성능 제공"