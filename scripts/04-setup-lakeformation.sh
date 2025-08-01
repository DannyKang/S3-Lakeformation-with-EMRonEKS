#!/bin/bash

# Lake Formation FGAC 권한 설정 스크립트 (Iceberg)
# Glue Catalog를 사용하는 Iceberg 테이블 방식

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



# 7. 권한 검증
echo ""
echo "7. 권한 검증..."

for role in "${ROLES[@]}"; do
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${role}"
    echo "   $role 권한 확인..."
    
    echo "aws lakeformation list-permissions --region $REGION --principal DataLakePrincipalIdentifier=$role_arn --resource "Database={Name=${DATABASE_NAME}}" --max-results 10 --query 'PrincipalResourcePermissions[*].{Resource:Resource,Permissions:Permissions}'"

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

