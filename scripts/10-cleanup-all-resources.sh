#!/bin/bash

# Lake Formation FGAC Demo - 전체 리소스 정리 스크립트
# 생성된 모든 AWS 리소스를 안전하게 삭제합니다
# 업데이트: 2025-08-04 - 실제 생성된 리소스에 맞게 수정

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 환경 변수 로드
if [ ! -f .env ]; then
    log_error ".env 파일이 없습니다. 프로젝트 루트 디렉토리에서 실행하세요."
    exit 1
fi

source .env

echo ""
echo "🧹 Lake Formation FGAC Demo - 전체 리소스 정리"
echo "=================================================="
echo ""
log_warning "이 스크립트는 다음 리소스들을 삭제합니다:"
echo "  • EMR on EKS Virtual Clusters"
echo "  • EMR Security Configurations"
echo "  • EKS Cluster ($CLUSTER_NAME)"
echo "  • Lake Formation 권한 및 필터"
echo "  • IAM 역할 및 정책"
echo "  • S3 버킷 및 데이터"
echo "  • Glue 데이터베이스 및 테이블"
echo "  • CloudWatch 로그 그룹"
echo ""

# 현재 환경 정보 표시
log_info "현재 환경 정보:"
echo "  • 계정 ID: $ACCOUNT_ID"
echo "  • 리전: $REGION"
echo "  • EKS 클러스터: $CLUSTER_NAME"
echo "  • Iceberg 버킷: $ICEBERG_BUCKET_NAME"
echo "  • Scripts 버킷: seoul-bike-analytics-scripts-$ACCOUNT_ID"
echo "  • Results 버킷: seoul-bike-analytics-results-$ACCOUNT_ID"
echo "  • Virtual Cluster: $LF_VIRTUAL_CLUSTER_ID"
echo ""

# 사용자 확인
read -p "정말로 모든 리소스를 삭제하시겠습니까? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    log_info "리소스 정리가 취소되었습니다."
    exit 0
fi

echo ""
log_info "리소스 정리를 시작합니다..."

# 1. EMR on EKS 리소스 정리
echo ""
log_info "1. EMR on EKS 리소스 정리 중..."

# 실행 중인 Job 확인 및 취소
log_info "실행 중인 EMR Job 확인 중..."

# 현재 Virtual Cluster에서 실행 중인 Job 확인
if [ ! -z "$LF_VIRTUAL_CLUSTER_ID" ]; then
    log_info "Virtual Cluster $LF_VIRTUAL_CLUSTER_ID의 실행 중인 Job 확인 중..."
    
    # 실행 중인 Job 목록 가져오기
    RUNNING_JOBS=$(aws emr-containers list-job-runs \
        --virtual-cluster-id "$LF_VIRTUAL_CLUSTER_ID" \
        --states PENDING SUBMITTED RUNNING \
        --region $REGION \
        --query 'jobRuns[].id' \
        --output text 2>/dev/null || echo "")
    
    if [ ! -z "$RUNNING_JOBS" ] && [ "$RUNNING_JOBS" != "None" ]; then
        log_warning "실행 중인 Job들을 취소합니다: $RUNNING_JOBS"
        for job_id in $RUNNING_JOBS; do
            aws emr-containers cancel-job-run \
                --virtual-cluster-id "$LF_VIRTUAL_CLUSTER_ID" \
                --id "$job_id" \
                --region $REGION 2>/dev/null || true
            log_info "Job $job_id 취소 요청 완료"
        done
        
        # Job 취소 완료 대기
        log_info "Job 취소 완료 대기 중... (30초)"
        sleep 30
    else
        log_info "실행 중인 Job이 없습니다."
    fi
fi

# 모든 Virtual Cluster 확인 및 삭제
log_info "모든 Virtual Cluster 확인 중..."
ALL_VCS=$(aws emr-containers list-virtual-clusters \
    --region $REGION \
    --query 'virtualClusters[?state==`RUNNING`].id' \
    --output text 2>/dev/null || echo "")

if [ ! -z "$ALL_VCS" ] && [ "$ALL_VCS" != "None" ]; then
    for vc_id in $ALL_VCS; do
        log_info "Virtual Cluster $vc_id 삭제 중..."
        aws emr-containers delete-virtual-cluster \
            --id "$vc_id" \
            --region $REGION 2>/dev/null || true
        log_success "Virtual Cluster $vc_id 삭제 요청 완료"
    done
else
    log_info "삭제할 Virtual Cluster가 없습니다."
fi

# Security Configuration 삭제
log_info "Security Configuration 삭제 중..."
ALL_SCS=$(aws emr-containers list-security-configurations \
    --region $REGION \
    --query 'securityConfigurations[].id' \
    --output text 2>/dev/null || echo "")

if [ ! -z "$ALL_SCS" ] && [ "$ALL_SCS" != "None" ]; then
    for sc_id in $ALL_SCS; do
        log_info "Security Configuration $sc_id 삭제 중..."
        aws emr-containers delete-security-configuration \
            --id "$sc_id" \
            --region $REGION 2>/dev/null || true
        log_success "Security Configuration $sc_id 삭제 요청 완료"
    done
else
    log_info "삭제할 Security Configuration이 없습니다."
fi

# 2. EKS 클러스터 정보 확인 (삭제하지 않음)
echo ""
log_info "2. EKS 클러스터 정보 확인..."

if [ ! -z "$CLUSTER_NAME" ]; then
    # 클러스터 존재 확인
    CLUSTER_STATUS=$(aws eks describe-cluster \
        --name "$CLUSTER_NAME" \
        --region $REGION \
        --query 'cluster.status' \
        --output text 2>/dev/null || echo "NOT_FOUND")
    
    if [ "$CLUSTER_STATUS" != "NOT_FOUND" ]; then
        log_info "EKS 클러스터 '$CLUSTER_NAME' 상태: $CLUSTER_STATUS"
        log_warning "EKS 클러스터는 수동으로 삭제해야 합니다."
        log_info "삭제 방법은 스크립트 완료 후 안내됩니다."
    else
        log_info "EKS 클러스터 '$CLUSTER_NAME'이 이미 삭제되었거나 존재하지 않습니다."
    fi
else
    log_warning "CLUSTER_NAME이 설정되지 않았습니다."
fi

# 3. Lake Formation 권한 정리
echo ""
log_info "3. Lake Formation 권한 정리 중..."

# Data Cells Filter 삭제
log_info "Data Cells Filter 삭제 중..."
FILTERS=(
    "gangnam_analytics_filter"
    "operation_team_filter"
    "marketing_partner_filter"
    "DataSteward-FullAccess"
)

for filter in "${FILTERS[@]}"; do
    aws lakeformation delete-data-cells-filter \
        --table-data '{
            "CatalogId": "'$ACCOUNT_ID'",
            "DatabaseName": "bike_db",
            "TableName": "bike_rental_data",
            "Name": "'$filter'"
        }' \
        --region $REGION 2>/dev/null || true
    log_info "Data Cells Filter $filter 삭제 완료"
done

# Lake Formation 권한 취소
log_info "Lake Formation 권한 취소 중..."
ROLES=(
    "LF_DataStewardRole"
    "LF_GangnamAnalyticsRole"
    "LF_OperationRole"
    "LF_MarketingPartnerRole"
)

for role in "${ROLES[@]}"; do
    log_info "역할 $role의 Lake Formation 권한 취소 중..."
    
    # 테이블 권한 취소
    aws lakeformation revoke-permissions \
        --principal "arn:aws:iam::$ACCOUNT_ID:role/$role" \
        --resource '{
            "Table": {
                "CatalogId": "'$ACCOUNT_ID'",
                "DatabaseName": "bike_db",
                "Name": "bike_rental_data"
            }
        }' \
        --permissions SELECT DESCRIBE \
        --region $REGION 2>/dev/null || true
    
    # 데이터베이스 권한 취소
    aws lakeformation revoke-permissions \
        --principal "arn:aws:iam::$ACCOUNT_ID:role/$role" \
        --resource '{
            "Database": {
                "CatalogId": "'$ACCOUNT_ID'",
                "Name": "bike_db"
            }
        }' \
        --permissions DESCRIBE \
        --region $REGION 2>/dev/null || true
    
    # S3 Location 권한 취소
    if [ ! -z "$ICEBERG_BUCKET_NAME" ]; then
        aws lakeformation revoke-permissions \
            --principal "arn:aws:iam::$ACCOUNT_ID:role/$role" \
            --resource '{
                "DataLocation": {
                    "CatalogId": "'$ACCOUNT_ID'",
                    "ResourceArn": "arn:aws:s3:::'$ICEBERG_BUCKET_NAME'/data/"
                }
            }' \
            --permissions DATA_LOCATION_ACCESS \
            --region $REGION 2>/dev/null || true
    fi
    
    log_info "역할 $role의 Lake Formation 권한 취소 완료"
done

log_success "Lake Formation 권한 정리 완료"

# 4. Glue 리소스 삭제
echo ""
log_info "4. Glue 리소스 삭제 중..."

# Glue 테이블 삭제
log_info "Glue 테이블 'bike_rental_data' 삭제 중..."
aws glue delete-table \
    --database-name "bike_db" \
    --name "bike_rental_data" \
    --region $REGION 2>/dev/null || true
log_success "Glue 테이블 삭제 완료"

# Glue 데이터베이스 삭제
log_info "Glue 데이터베이스 'bike_db' 삭제 중..."
aws glue delete-database \
    --name "bike_db" \
    --region $REGION 2>/dev/null || true
log_success "Glue 데이터베이스 삭제 완료"

# 5. S3 버킷 삭제
echo ""
log_info "5. S3 버킷 삭제 중..."

# Iceberg 데이터 버킷 삭제
if [ ! -z "$ICEBERG_BUCKET_NAME" ]; then
    log_info "S3 버킷 '$ICEBERG_BUCKET_NAME' 삭제 중..."
    
    # 버킷 존재 확인
    if aws s3api head-bucket --bucket "$ICEBERG_BUCKET_NAME" --region $REGION 2>/dev/null; then
        # 버킷 버전 관리 확인 및 모든 객체 버전 삭제
        log_info "버킷 내 모든 객체 및 버전 삭제 중..."
        
        # 모든 객체 삭제
        aws s3 rm "s3://$ICEBERG_BUCKET_NAME" --recursive --region $REGION 2>/dev/null || true
        
        # 버킷 삭제
        aws s3api delete-bucket \
            --bucket "$ICEBERG_BUCKET_NAME" \
            --region $REGION 2>/dev/null || true
        
        log_success "S3 버킷 '$ICEBERG_BUCKET_NAME' 삭제 완료"
    else
        log_info "S3 버킷 '$ICEBERG_BUCKET_NAME'이 이미 삭제되었거나 존재하지 않습니다."
    fi
fi

# Scripts 버킷 삭제
SCRIPTS_BUCKET_NAME="seoul-bike-analytics-scripts-$ACCOUNT_ID"
log_info "S3 버킷 '$SCRIPTS_BUCKET_NAME' 삭제 중..."
if aws s3api head-bucket --bucket "$SCRIPTS_BUCKET_NAME" --region $REGION 2>/dev/null; then
    aws s3 rm "s3://$SCRIPTS_BUCKET_NAME" --recursive --region $REGION 2>/dev/null || true
    aws s3api delete-bucket \
        --bucket "$SCRIPTS_BUCKET_NAME" \
        --region $REGION 2>/dev/null || true
    log_success "S3 버킷 '$SCRIPTS_BUCKET_NAME' 삭제 완료"
else
    log_info "S3 버킷 '$SCRIPTS_BUCKET_NAME'이 이미 삭제되었거나 존재하지 않습니다."
fi

# Results 버킷 삭제
RESULTS_BUCKET_NAME="seoul-bike-analytics-results-$ACCOUNT_ID"
log_info "S3 버킷 '$RESULTS_BUCKET_NAME' 삭제 중..."
if aws s3api head-bucket --bucket "$RESULTS_BUCKET_NAME" --region $REGION 2>/dev/null; then
    aws s3 rm "s3://$RESULTS_BUCKET_NAME" --recursive --region $REGION 2>/dev/null || true
    aws s3api delete-bucket \
        --bucket "$RESULTS_BUCKET_NAME" \
        --region $REGION 2>/dev/null || true
    log_success "S3 버킷 '$RESULTS_BUCKET_NAME' 삭제 완료"
else
    log_info "S3 버킷 '$RESULTS_BUCKET_NAME'이 이미 삭제되었거나 존재하지 않습니다."
fi

# 6. IAM 역할 및 정책 삭제
echo ""
log_info "6. IAM 역할 및 정책 삭제 중..."

ROLES=(
    "LF_DataStewardRole"
    "LF_GangnamAnalyticsRole" 
    "LF_OperationRole"
    "LF_MarketingPartnerRole"
    "LF_QueryEngineRole"
    "LF_JobExecutionRole"
)

for role in "${ROLES[@]}"; do
    log_info "IAM 역할 '$role' 삭제 중..."
    
    # 역할 존재 확인
    if aws iam get-role --role-name "$role" --region $REGION >/dev/null 2>&1; then
        # 인라인 정책 삭제
        INLINE_POLICIES=$(aws iam list-role-policies \
            --role-name "$role" \
            --query 'PolicyNames' \
            --output text \
            --region $REGION 2>/dev/null || echo "")
        
        for policy in $INLINE_POLICIES; do
            if [ "$policy" != "None" ] && [ ! -z "$policy" ]; then
                aws iam delete-role-policy \
                    --role-name "$role" \
                    --policy-name "$policy" \
                    --region $REGION 2>/dev/null || true
                log_info "인라인 정책 $policy 삭제 완료"
            fi
        done
        
        # 관리형 정책 분리
        ATTACHED_POLICIES=$(aws iam list-attached-role-policies \
            --role-name "$role" \
            --query 'AttachedPolicies[].PolicyArn' \
            --output text \
            --region $REGION 2>/dev/null || echo "")
        
        for policy_arn in $ATTACHED_POLICIES; do
            if [ "$policy_arn" != "None" ] && [ ! -z "$policy_arn" ]; then
                aws iam detach-role-policy \
                    --role-name "$role" \
                    --policy-arn "$policy_arn" \
                    --region $REGION 2>/dev/null || true
                log_info "관리형 정책 $policy_arn 분리 완료"
            fi
        done
        
        # 역할 삭제
        aws iam delete-role \
            --role-name "$role" \
            --region $REGION 2>/dev/null || true
        
        log_success "IAM 역할 '$role' 삭제 완료"
    else
        log_info "IAM 역할 '$role'이 이미 삭제되었거나 존재하지 않습니다."
    fi
done

# 7. CloudWatch 로그 그룹 삭제
echo ""
log_info "7. CloudWatch 로그 그룹 삭제 중..."

LOG_GROUPS=(
    "/aws/emr-containers/jobs"
    "/aws/emr-containers/seoul-bike"
    "/aws/eks/$CLUSTER_NAME/cluster"
)

for log_group in "${LOG_GROUPS[@]}"; do
    log_info "로그 그룹 '$log_group' 삭제 중..."
    aws logs delete-log-group \
        --log-group-name "$log_group" \
        --region $REGION 2>/dev/null || true
    log_success "로그 그룹 '$log_group' 삭제 완료"
done

# 8. 로컬 파일 정리
echo ""
log_info "8. 로컬 파일 정리 중..."

# 생성된 임시 파일들 삭제
log_info "생성된 결과 파일들 정리 중..."
rm -rf results/ 2>/dev/null || true
rm -rf job-templates/ 2>/dev/null || true
rm -rf pod-templates/ 2>/dev/null || true
rm -rf /tmp/*job*.json 2>/dev/null || true
rm -rf /tmp/*template*.yaml 2>/dev/null || true
rm -rf /tmp/analyze*.py 2>/dev/null || true

# .env 파일 백업 후 정리
if [ -f .env ]; then
    BACKUP_FILE=".env.backup.$(date +%Y%m%d_%H%M%S)"
    cp .env "$BACKUP_FILE"
    log_info ".env 파일을 백업했습니다: $BACKUP_FILE"
fi

log_success "로컬 파일 정리 완료"

# 완료 메시지
echo ""
echo "🎉 리소스 정리 완료!"
echo "=================================================="
echo ""
log_success "다음 리소스들이 삭제되었습니다:"
echo "  ✅ EMR on EKS Virtual Clusters 및 Security Configurations"
echo "  ✅ Lake Formation 권한 및 Data Cells Filters"
echo "  ✅ Glue 데이터베이스 및 테이블 (bike_db.bike_rental_data)"
echo "  ✅ S3 버킷 및 모든 데이터:"
echo "     • $ICEBERG_BUCKET_NAME"
echo "     • seoul-bike-analytics-scripts-$ACCOUNT_ID"
echo "     • seoul-bike-analytics-results-$ACCOUNT_ID"
echo "  ✅ IAM 역할 및 정책 (6개 역할)"
echo "  ✅ CloudWatch 로그 그룹"
echo "  ✅ 로컬 임시 파일"
echo ""
log_warning "주의사항:"
echo "  • EKS 클러스터 '$CLUSTER_NAME'은 수동으로 삭제해야 합니다"
echo "  • Data-on-EKS Blueprint를 사용한 경우 Terraform destroy 실행 필요"
echo "  • AWS 콘솔에서 남은 리소스를 확인하세요"
echo "  • .env 파일이 백업되었습니다"
echo ""
log_info "수동 정리가 필요한 리소스:"
echo "  • EKS 클러스터: $CLUSTER_NAME"
echo "  • VPC 및 서브넷 (자동 생성된 경우)"
echo "  • Security Groups"
echo "  • Load Balancers"
echo "  • NAT Gateways"
echo "  • EC2 인스턴스 (Karpenter 관리)"
echo ""
log_info "AWS 콘솔에서 다음을 확인하세요:"
echo "  • EKS 클러스터: https://console.aws.amazon.com/eks/home?region=$REGION#/clusters"
echo "  • EMR on EKS: https://console.aws.amazon.com/emr/home?region=$REGION#/containers"
echo "  • S3 버킷: https://console.aws.amazon.com/s3/home?region=$REGION"
echo "  • EC2 인스턴스: https://console.aws.amazon.com/ec2/home?region=$REGION#Instances"
echo "  • IAM 역할: https://console.aws.amazon.com/iam/home#/roles"
echo ""
log_info "EKS 클러스터 수동 삭제 방법:"
echo "  1. Data-on-EKS Blueprint 사용한 경우:"
echo "     cd data-on-eks-blueprint/analytics/terraform/emr-eks-karpenter"
echo "     terraform destroy -auto-approve"
echo ""
echo "  2. AWS CLI 사용한 경우:"
echo "     aws eks delete-cluster --name $CLUSTER_NAME --region $REGION"
echo ""
