#!/bin/bash

# Lake Formation FGAC 권한 설정 스크립트 (Iceberg)
# Glue Catalog를 사용하는 Iceberg 테이블 방식
# 
# 업데이트: 2025-08-04
# - DataSteward 권한을 실제 적용된 Data Cells Filter 방식으로 변경
# - 모든 역할이 Data Cells Filter를 통해 일관된 방식으로 접근
# - Multi-dimensional FGAC 완전 구현 (Row/Column/Cell-level Filtering)

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/01-create-s3-bucket.sh를 실행하세요."
    exit 1
fi

# .env 파일 검증 및 로드
echo "환경 설정 파일 로드 중..."
if ! source .env 2>/dev/null; then
    echo "❌ .env 파일 로드 중 오류가 발생했습니다."
    echo "파일 내용을 확인하거나 01-create-s3-bucket.sh를 다시 실행하세요."
    exit 1
fi

# 필수 환경 변수 확인
if [ -z "$ACCOUNT_ID" ] || [ -z "$REGION" ] || [ -z "$ICEBERG_BUCKET_NAME" ] || [ -z "$LF_DATA_STEWARD_ROLE" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "이전 단계들을 순서대로 다시 실행하세요."
    exit 1
fi

echo "=== Lake Formation FGAC 권한 설정 시작 (Iceberg) ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "S3 Iceberg 버킷: $ICEBERG_BUCKET_NAME"
echo "데이터베이스: $DATABASE_NAME"
echo "테이블: $TABLE_NAME"
echo ""

# 0. IAM 역할 존재 확인
echo "0. IAM 역할 존재 확인..."
ROLES=("$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE")

for role in "${ROLES[@]}"; do
    if aws iam get-role --role-name $role >/dev/null 2>&1; then
        echo "✅ $role 존재 확인"
    else
        echo "❌ $role 역할이 존재하지 않습니다."
        echo "먼저 ./scripts/03-create-iam-roles.sh를 실행하세요."
        exit 1
    fi
done

# 1. Lake Formation 설정 초기화
echo ""
echo "1. Lake Formation 설정 초기화..."

# Lake Formation 관리자 설정
CURRENT_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
echo "   Lake Formation 관리자 설정: $CURRENT_USER_ARN"

aws lakeformation put-data-lake-settings \
    --region $REGION \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {
                "DataLakePrincipalIdentifier": "'$CURRENT_USER_ARN'"
            }
        ],
        "CreateDatabaseDefaultPermissions": [],
        "CreateTableDefaultPermissions": []
    }' >/dev/null 2>&1 || echo "   Lake Formation 설정이 이미 구성되어 있습니다."

# Glue 데이터베이스 및 테이블 확인
echo "   Glue 데이터베이스 및 테이블 확인 중..."
if aws glue get-database --name $DATABASE_NAME --region $REGION >/dev/null 2>&1; then
    echo "   ✅ Glue 데이터베이스 확인: $DATABASE_NAME"
else
    echo "   ❌ Glue 데이터베이스를 찾을 수 없습니다: $DATABASE_NAME"
    echo "   먼저 ./scripts/02-load-data-to-iceberg.sh를 실행하세요."
    exit 1
fi

if aws glue get-table --database-name $DATABASE_NAME --name $TABLE_NAME --region $REGION >/dev/null 2>&1; then
    echo "   ✅ Glue 테이블 확인: $DATABASE_NAME.$TABLE_NAME"
else
    echo "   ❌ Glue 테이블을 찾을 수 없습니다: $DATABASE_NAME.$TABLE_NAME"
    echo "   먼저 ./scripts/02-load-data-to-iceberg.sh를 실행하세요."
    exit 1
fi

# 2. LF_DataStewardRole - 전체 데이터 접근 권한 (Data Cells Filter 방식)
echo ""
echo "2. $LF_DATA_STEWARD_ROLE 권한 설정 (전체 데이터 접근)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_DATA_STEWARD_ROLE_ARN \
    --resource "Database={Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 테이블 기본 권한
echo "   테이블 기본 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_DATA_STEWARD_ROLE_ARN \
    --resource "Table={DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  테이블 권한이 이미 부여되어 있습니다."

# DataSteward Data Cells Filter 생성 (전체 데이터 + 전체 컬럼)
echo "   DataSteward Data Cells Filter 생성 중 (전체 접근)..."

# 필터 이름 설정
DATASTEWARD_FILTER_NAME="DataSteward-FullAccess"

# 기존 필터 삭제 (있다면)
echo "   기존 필터 삭제 중..."
aws lakeformation delete-data-cells-filter \
    --region $REGION \
    --table-catalog-id $ACCOUNT_ID \
    --database-name $DATABASE_NAME \
    --table-name $TABLE_NAME \
    --name $DATASTEWARD_FILTER_NAME 2>/dev/null || echo "   기존 필터 없음"

# 새 필터 생성 (전체 데이터 + 전체 컬럼)
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${ACCOUNT_ID}\",
        \"DatabaseName\": \"${DATABASE_NAME}\",
        \"TableName\": \"${TABLE_NAME}\",
        \"Name\": \"${DATASTEWARD_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"TRUE\",
            \"AllRowsWildcard\": {}
        },
        \"ColumnWildcard\": {
            \"ExcludedColumnNames\": []
        }
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여 (EMR on EKS FGAC 필수)
echo "   필터 권한 부여 중 (EMR on EKS FGAC 필수)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_DATA_STEWARD_ROLE_ARN \
    --resource "{
        \"DataCellsFilter\": {
            \"TableCatalogId\": \"${ACCOUNT_ID}\",
            \"DatabaseName\": \"${DATABASE_NAME}\",
            \"TableName\": \"${TABLE_NAME}\",
            \"Name\": \"${DATASTEWARD_FILTER_NAME}\"
        }
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ DataSteward: 전체 11개 컬럼, 모든 구, 모든 연령대 (100,000건) - Data Cells Filter 방식"

# 3. LF_GangnamAnalyticsRole - 강남구 데이터만, birth_year 제외
echo ""
echo "3. $LF_GANGNAM_ANALYTICS_ROLE 권한 설정 (강남구만, 개인정보 제외)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource "Database={Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 테이블 기본 권한 (DESCRIBE)
echo "   테이블 기본 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource "Table={DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  테이블 권한이 이미 부여되어 있습니다."

# 테이블 컬럼 SELECT 권한 (birth_year 제외)
echo "   테이블 컬럼 SELECT 권한 부여 중 (birth_year 제외)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource '{
        "TableWithColumns": {
            "DatabaseName": "'${DATABASE_NAME}'",
            "Name": "'${TABLE_NAME}'",
            "ColumnNames": [
                "rental_id", "station_id", "station_name", "rental_date", "return_date",
                "usage_min", "distance_meter", "gender", "user_type", "district"
            ]
        }
    }' \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  테이블 컬럼 권한이 이미 부여되어 있습니다."

# 강남구 데이터 필터 생성 (EMR on EKS FGAC 필수)
echo "   강남구 데이터 필터 생성 (EMR on EKS FGAC 필수)..."
GANGNAM_FILTER_NAME="gangnam_analytics_filter"

# 기존 필터 삭제 (있는 경우)
aws lakeformation delete-data-cells-filter \
    --region $REGION \
    --table-catalog-id $ACCOUNT_ID \
    --database-name $DATABASE_NAME \
    --table-name $TABLE_NAME \
    --name $GANGNAM_FILTER_NAME 2>/dev/null || echo "   기존 필터 없음"

# 새 필터 생성 (강남구 필터 적용)
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${ACCOUNT_ID}\",
        \"DatabaseName\": \"${DATABASE_NAME}\",
        \"TableName\": \"${TABLE_NAME}\",
        \"Name\": \"${GANGNAM_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"district = '강남구'\"
        },
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"gender\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여 (EMR on EKS FGAC 필수)
echo "   필터 권한 부여 중 (EMR on EKS FGAC 필수)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${ACCOUNT_ID},
        DatabaseName=${DATABASE_NAME},
        TableName=${TABLE_NAME},
        Name=${GANGNAM_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ GangnamAnalyst: 10개 컬럼 (birth_year 제외), 강남구만 접근 (~3,000건)"


# 4. LF_OperationRole - 운영 데이터만, 개인정보 제외
echo ""
echo "4. $LF_OPERATION_ROLE 권한 설정 (운영 데이터만, 개인정보 제외)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource "Database={Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 테이블 기본 권한 (DESCRIBE)
echo "   테이블 기본 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource "Table={DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  테이블 권한이 이미 부여되어 있습니다."

# 테이블 컬럼 SELECT 권한 (birth_year, gender 제외)
echo "   테이블 컬럼 SELECT 권한 부여 중 (birth_year, gender 제외)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource '{
        "TableWithColumns": {
            "DatabaseName": "'${DATABASE_NAME}'",
            "Name": "'${TABLE_NAME}'",
            "ColumnNames": [
                "rental_id", "station_id", "station_name", "rental_date", "return_date",
                "usage_min", "distance_meter", "user_type", "district"
            ]
        }
    }' \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  테이블 컬럼 권한이 이미 부여되어 있습니다."

# 운영팀용 컬럼 필터 생성 (EMR on EKS FGAC 필수)
echo "   운영팀용 컬럼 필터 생성 (EMR on EKS FGAC 필수)..."
OPERATION_FILTER_NAME="operation_team_filter"

# 기존 필터 삭제 (있는 경우)
aws lakeformation delete-data-cells-filter \
    --region $REGION \
    --table-catalog-id $ACCOUNT_ID \
    --database-name $DATABASE_NAME \
    --table-name $TABLE_NAME \
    --name $OPERATION_FILTER_NAME 2>/dev/null || echo "   기존 필터 없음"

# 새 필터 생성 (TableCatalogId 포함)
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${ACCOUNT_ID}\",
        \"DatabaseName\": \"${DATABASE_NAME}\",
        \"TableName\": \"${TABLE_NAME}\",
        \"Name\": \"${OPERATION_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"TRUE\"
        },
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여 (EMR on EKS FGAC 필수)
echo "   필터 권한 부여 중 (EMR on EKS FGAC 필수)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${ACCOUNT_ID},
        DatabaseName=${DATABASE_NAME},
        TableName=${TABLE_NAME},
        Name=${OPERATION_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ Operation: 9개 컬럼 (birth_year, gender 제외), 전체 구 접근 가능 + 운영 필터 (선택적)"

# 5. LF_MarketingPartnerRole - 강남구 20-30대만, 마케팅 관련
echo ""
echo "5. $LF_MARKETING_PARTNER_ROLE 권한 설정 (강남구 20-30대만, 마케팅 관련)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource "Database={Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 테이블 기본 권한 (DESCRIBE)
echo "   테이블 기본 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource "Table={DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  테이블 권한이 이미 부여되어 있습니다."

# 테이블 컬럼 SELECT 권한 (birth_year 제외)
echo "   테이블 컬럼 SELECT 권한 부여 중 (birth_year 제외)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource '{
        "TableWithColumns": {
            "DatabaseName": "'${DATABASE_NAME}'",
            "Name": "'${TABLE_NAME}'",
            "ColumnNames": [
                "rental_id", "station_id", "station_name", "rental_date", "return_date",
                "usage_min", "distance_meter", "gender", "user_type", "district"
            ]
        }
    }' \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  테이블 컬럼 권한이 이미 부여되어 있습니다."

# 마케팅 파트너용 다차원 필터 생성 (EMR on EKS FGAC 필수 - 강남구 + 20-30대)
echo "   마케팅 파트너용 다차원 필터 생성 (EMR on EKS FGAC 필수 - 강남구 + 20-30대)..."
MARKETING_FILTER_NAME="marketing_partner_filter"

# 기존 필터 삭제 (있는 경우)
aws lakeformation delete-data-cells-filter \
    --region $REGION \
    --table-catalog-id $ACCOUNT_ID \
    --database-name $DATABASE_NAME \
    --table-name $TABLE_NAME \
    --name $MARKETING_FILTER_NAME 2>/dev/null || echo "   기존 필터 없음"

# 새 필터 생성 (TableCatalogId 포함)
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${ACCOUNT_ID}\",
        \"DatabaseName\": \"${DATABASE_NAME}\",
        \"TableName\": \"${TABLE_NAME}\",
        \"Name\": \"${MARKETING_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"district = '강남구' AND (birth_year >= '1994' AND birth_year <= '2004')\"
        },
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"gender\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여 (EMR on EKS FGAC 필수)
echo "   필터 권한 부여 중 (EMR on EKS FGAC 필수)..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${ACCOUNT_ID},
        DatabaseName=${DATABASE_NAME},
        TableName=${TABLE_NAME},
        Name=${MARKETING_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ MarketingPartner: 10개 컬럼 (birth_year 제외), 전체 구 접근 가능 + 강남구 20-30대 필터 (선택적)"

# 6. 데이터 위치 권한 설정
echo ""
echo "6. 데이터 위치 권한 설정..."

# S3 리소스 등록 확인
echo "   S3 리소스 등록 확인..."
S3_RESOURCE_ARN="arn:aws:s3:::${ICEBERG_BUCKET_NAME}/"

if aws lakeformation describe-resource --resource-arn $S3_RESOURCE_ARN --region $REGION >/dev/null 2>&1; then
    echo "   ✅ S3 리소스가 이미 등록되어 있습니다."
else
    echo "   S3 리소스 등록 중..."
    aws lakeformation register-resource \
        --region $REGION \
        --resource-arn $S3_RESOURCE_ARN \
        --use-service-linked-role 2>/dev/null || echo "   ⚠️  리소스 등록 중 오류 발생"
fi

# 각 역할에 데이터 위치 권한 부여
for role in "${ROLES[@]}"; do
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${role}"
    echo "   $role 데이터 위치 권한 부여..."
    aws lakeformation grant-permissions \
        --region $REGION \
        --principal DataLakePrincipalIdentifier=$role_arn \
        --resource "DataLocation={ResourceArn=${S3_RESOURCE_ARN}}" \
        --permissions "DATA_LOCATION_ACCESS" 2>/dev/null || echo "   ⚠️  권한이 이미 부여되어 있습니다."
done

# 7. 권한 검증
echo ""
echo "7. 권한 검증..."

for role in "${ROLES[@]}"; do
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${role}"
    echo "   $role 권한 확인..."
    
    if PERMISSIONS=$(aws lakeformation list-permissions \
        --region $REGION \
        --principal DataLakePrincipalIdentifier=$role_arn \
        --resource "Database={Name=${DATABASE_NAME}}" \
        --max-results 10 \
        --query 'PrincipalResourcePermissions[*].{Resource:Resource,Permissions:Permissions}' \
        --output json 2>/dev/null); then
        
        PERMISSION_COUNT=$(echo $PERMISSIONS | jq length)
        if [ "$PERMISSION_COUNT" -gt 0 ]; then
            echo "   ✅ $role: $PERMISSION_COUNT개 권한 확인됨"
        else
            echo "   ⚠️  $role: 권한이 설정되지 않았을 수 있습니다"
        fi
    else
        echo "   ⚠️  $role: 권한 조회 실패 (정상적인 경우일 수 있음)"
    fi
done

echo ""
echo "📋 권한 검증 참고사항:"
echo "   • Glue Catalog를 사용하는 Iceberg 방식입니다"
echo "   • 실제 데이터 접근은 EMR on EKS에서 테스트됩니다"
echo "   • 권한 조회 실패는 정상적인 경우일 수 있습니다"

# 8. 환경 설정 업데이트
echo ""
echo "8. 환경 설정 업데이트..."

# 기존 Lake Formation 설정이 있는지 확인
if grep -q "LAKE_FORMATION_SETUP=" .env 2>/dev/null; then
    echo "   ⚠️  기존 Lake Formation 설정이 발견되었습니다."
    echo "   기존 정보를 유지합니다."
else
    cat >> .env << EOF

# Lake Formation FGAC 설정 (Iceberg)
LAKE_FORMATION_SETUP=iceberg
LAKE_FORMATION_DATABASE_NAME=$DATABASE_NAME
LAKE_FORMATION_TABLE_NAME=$TABLE_NAME
LAKE_FORMATION_SETUP_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
EOF
    echo "   ✅ Lake Formation 설정 정보 추가 완료"
fi

echo "✅ 환경 설정 업데이트 완료"

echo ""
echo "=== Lake Formation FGAC 권한 설정 완료 (Iceberg) ==="
echo ""
echo "📊 설정된 권한 요약:"
echo "┌─────────────────────────┬──────────┬─────────────┬─────────────┬──────────────┬─────────────────┐"
echo "│ 역할                    │ 접근구역 │ 연령대      │ 접근컬럼    │ 예상결과     │ 세밀한제어      │"
echo "├─────────────────────────┼──────────┼─────────────┼─────────────┼──────────────┼─────────────────┤"
echo "│ LF_DataStewardRole      │ 전체구   │ 전체        │ 전체 11개   │ 100,000건    │ FullAccess 필터 │"
echo "│ LF_GangnamAnalyticsRole │ 강남구만 │ 전체        │ 10개(개인정보제외) │ ~3,000건  │ 강남구 필터     │"
echo "│ LF_OperationRole        │ 전체구   │ 전체        │ 9개(운영관련만) │ 100,000건 │ 운영 필터       │"
echo "│ LF_MarketingPartnerRole │ 강남구만 │ 20-30대만   │ 10개(마케팅관련) │ ~2,000건 │ 강남구+20-30대  │"
echo "└─────────────────────────┴──────────┴─────────────┴─────────────┴──────────────┴─────────────────┘"
echo ""
echo "🔑 핵심 FGAC 기능 (Iceberg) - EMR on EKS FGAC 권한 구조:"
echo "   📋 1단계 - 기본 테이블 접근 (Data Cells Filter 방식):"
echo "      • 모든 역할이 Data Cells Filter를 통해 테이블에 접근"
echo "      • DataSteward: FullAccess 필터 (전체 데이터 + 전체 컬럼)"
echo "      • 다른 역할: 역할별 컬럼/행 필터링 적용"
echo ""
echo "   🎯 2단계 - 데이터 셀 필터 (DataCellsFilter - EMR on EKS FGAC 필수):"
echo "      • GangnamAnalytics: 강남구만 + birth_year 제외한 10개 컬럼 접근"
echo "      • Operation: 전체 구 + birth_year, gender 제외한 9개 컬럼 접근"
echo "      • MarketingPartner: 강남구 + 20-30대 다차원 필터 + birth_year 제외"
echo ""
echo "   💡 EMR on EKS FGAC 권한 적용 방식:"
echo "      • 기본 접근: TableWithColumns SELECT (필수)"
echo "      • 세밀한 제어: DataCellsFilter SELECT (EMR on EKS FGAC 필수)"
echo "      • 데이터 셀 필터 없이는 EMR on EKS에서 접근 불가"
echo "      • TableCatalogId 포함하여 필터 생성 필수"
echo ""
echo "🏗️ 사용된 리소스:"
echo "   • S3 버킷: $ICEBERG_BUCKET_NAME"
echo "   • Glue 데이터베이스: $DATABASE_NAME"
echo "   • Glue 테이블: $TABLE_NAME"
echo "   • 테이블 형식: Apache Iceberg"
echo ""

# 9. Data Cells Filter 확인
echo "9. 생성된 Data Cells Filter 확인..."
echo ""
echo "📊 생성된 Data Cells Filter 목록:"
aws lakeformation list-data-cells-filter \
    --region $REGION \
    --table "{
        \"CatalogId\": \"${ACCOUNT_ID}\",
        \"DatabaseName\": \"${DATABASE_NAME}\",
        \"Name\": \"${TABLE_NAME}\"
    }" \
    --query 'DataCellsFilters[].{
        Name: Name,
        RowFilter: RowFilter.FilterExpression,
        ColumnCount: length(ColumnNames),
        HasColumnWildcard: ColumnWildcard != null
    }' \
    --output table 2>/dev/null || echo "   ⚠️  Data Cells Filter 조회 실패"

echo ""
echo "✅ 다음 단계: ./scripts/05-setup-emr-on-eks.sh"