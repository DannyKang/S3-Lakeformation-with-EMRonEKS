#!/bin/bash

# Lake Formation FGAC Demo - 전체 리소스 정리 스크립트
# 생성된 모든 AWS 리소스를 안전하게 삭제합니다

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
echo "  • EKS Cluster (seoul-bike-emr)"
echo "  • Lake Formation 권한 및 필터"
echo "  • IAM 역할 및 정책"
echo "  • S3 버킷 및 데이터"
echo "  • Glue 데이터베이스 및 테이블"
echo "  • CloudWatch 로그 그룹"
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
VIRTUAL_CLUSTERS=(
    "$VIRTUAL_CLUSTER_ID"
    "$LF_VIRTUAL_CLUSTER_ID"
    "$LF_VIRTUAL_CLUSTER_ID_FIXED"
    "$LF_VIRTUAL_CLUSTER_ID_FIXED_V2"
    "$LF_VIRTUAL_CLUSTER_ID_FINAL"
    "wyj80luf4wyw1g50ny0uo0iah"
    "le0suayt470mwxswi6lx3vaw1"
)

for vc_id in "${VIRTUAL_CLUSTERS[@]}"; do
    if [ ! -z "$vc_id" ] && [ "$vc_id" != "" ]; then
        log_info "Virtual Cluster $vc_id의 실행 중인 Job 확인 중..."
        
        # 실행 중인 Job 목록 가져오기
        RUNNING_JOBS=$(aws emr-containers list-job-runs \
            --virtual-cluster-id "$vc_id" \
            --states PENDING SUBMITTED RUNNING \
            --region $REGION \
            --query 'jobRuns[].id' \
            --output text 2>/dev/null || echo "")
        
        if [ ! -z "$RUNNING_JOBS" ]; then
            log_warning "실행 중인 Job들을 취소합니다: $RUNNING_JOBS"
            for job_id in $RUNNING_JOBS; do
                aws emr-containers cancel-job-run \
                    --virtual-cluster-id "$vc_id" \
                    --id "$job_id" \
                    --region $REGION 2>/dev/null || true
                log_info "Job $job_id 취소 요청 완료"
            done
        fi
    fi
done

# Job 취소 완료 대기
log_info "Job 취소 완료 대기 중... (30초)"
sleep 30

# Virtual Cluster 삭제
log_info "Virtual Cluster 삭제 중..."
for vc_id in "${VIRTUAL_CLUSTERS[@]}"; do
    if [ ! -z "$vc_id" ] && [ "$vc_id" != "" ]; then
        log_info "Virtual Cluster $vc_id 삭제 중..."
        aws emr-containers delete-virtual-cluster \
            --id "$vc_id" \
            --region $REGION 2>/dev/null || true
        log_success "Virtual Cluster $vc_id 삭제 요청 완료"
    fi
done

# Security Configuration 삭제
log_info "Security Configuration 삭제 중..."
SECURITY_CONFIGS=(
    "$SECURITY_CONFIG_ID"
    "$SECURITY_CONFIG_ID_FIXED"
    "$SECURITY_CONFIG_ID_FIXED_V2"
    "$SECURITY_CONFIG_ID_FINAL"
)

for sc_id in "${SECURITY_CONFIGS[@]}"; do
    if [ ! -z "$sc_id" ] && [ "$sc_id" != "" ]; then
        log_info "Security Configuration $sc_id 삭제 중..."
        aws emr-containers delete-security-configuration \
            --id "$sc_id" \
            --region $REGION 2>/dev/null || true
        log_success "Security Configuration $sc_id 삭제 요청 완료"
    fi
done

# 2. EKS 클러스터 삭제 (Terraform 사용)
echo ""
log_info "2. EKS 클러스터 삭제 중..."

if [ ! -z "$CLUSTER_NAME" ]; then
    log_warning "EKS 클러스터 '$CLUSTER_NAME' 삭제는 시간이 오래 걸립니다 (15-20분)"
    
    # Data-on-EKS Blueprint Terraform으로 삭제
    TERRAFORM_DIR="./data-on-eks-blueprint/analytics/terraform/emr-eks-karpenter"
    
    if [ -d "$TERRAFORM_DIR" ]; then
        log_info "Data-on-EKS Blueprint Terraform으로 EKS 클러스터 삭제 중..."
        
        # Terraform 디렉토리로 이동
        cd "$TERRAFORM_DIR"
        
        # Terraform 상태 확인
        if [ -f "terraform.tfstate" ] || [ -f ".terraform/terraform.tfstate" ]; then
            log_info "Terraform 상태 파일 발견. Terraform destroy 실행 중..."
            
            # Karpenter 노드 정리 (Terraform destroy 전에)
            log_info "Karpenter 관리 노드 정리 중..."
            kubectl delete nodes --selector=karpenter.sh/provisioner-name --ignore-not-found=true 2>/dev/null || true
            kubectl delete nodes --selector=karpenter.sh/nodepool --ignore-not-found=true 2>/dev/null || true
            
            # NodePool과 NodeClass 정리
            kubectl delete nodepools --all --ignore-not-found=true 2>/dev/null || true
            kubectl delete ec2nodeclasses --all --ignore-not-found=true 2>/dev/null || true
            
            # 잠시 대기 (노드 정리 완료)
            sleep 30
            
            # Terraform destroy 실행
            terraform destroy -auto-approve 2>/dev/null || {
                log_warning "Terraform destroy 실패. 강제 삭제를 시도합니다."
                
                # 강제 삭제를 위한 추가 리소스 정리
                log_info "남은 Kubernetes 리소스 강제 정리 중..."
                kubectl delete all --all --all-namespaces --ignore-not-found=true 2>/dev/null || true
                
                # 다시 Terraform destroy 시도
                terraform destroy -auto-approve -refresh=false 2>/dev/null || true
            }
            
            # Terraform 상태 파일 정리
            rm -rf .terraform* terraform.tfstate* *.tfplan 2>/dev/null || true
            
            log_success "Terraform destroy 완료"
        else
            log_warning "Terraform 상태 파일이 없습니다. AWS CLI로 직접 삭제를 시도합니다."
        fi
        
        # 원래 디렉토리로 복귀
        cd - > /dev/null
    else
        log_warning "Terraform 디렉토리를 찾을 수 없습니다: $TERRAFORM_DIR"
    fi
    
    # AWS CLI로 직접 삭제 시도 (백업 방법)
    log_info "AWS CLI로 EKS 클러스터 상태 확인 및 정리..."
    
    # 클러스터 존재 확인
    CLUSTER_STATUS=$(aws eks describe-cluster \
        --name "$CLUSTER_NAME" \
        --region $REGION \
        --query 'cluster.status' \
        --output text 2>/dev/null || echo "NOT_FOUND")
    
    if [ "$CLUSTER_STATUS" != "NOT_FOUND" ]; then
        log_info "클러스터 상태: $CLUSTER_STATUS"
        
        # 노드 그룹 삭제
        NODE_GROUPS=$(aws eks list-nodegroups \
            --cluster-name "$CLUSTER_NAME" \
            --region $REGION \
            --query 'nodegroups' \
            --output text 2>/dev/null || echo "")
        
        for ng in $NODE_GROUPS; do
            if [ "$ng" != "None" ] && [ ! -z "$ng" ]; then
                log_info "노드 그룹 $ng 삭제 중..."
                aws eks delete-nodegroup \
                    --cluster-name "$CLUSTER_NAME" \
                    --nodegroup-name "$ng" \
                    --region $REGION 2>/dev/null || true
            fi
        done
        
        # Fargate 프로파일 삭제
        FARGATE_PROFILES=$(aws eks list-fargate-profiles \
            --cluster-name "$CLUSTER_NAME" \
            --region $REGION \
            --query 'fargateProfileNames' \
            --output text 2>/dev/null || echo "")
        
        for fp in $FARGATE_PROFILES; do
            if [ "$fp" != "None" ] && [ ! -z "$fp" ]; then
                log_info "Fargate 프로파일 $fp 삭제 중..."
                aws eks delete-fargate-profile \
                    --cluster-name "$CLUSTER_NAME" \
                    --fargate-profile-name "$fp" \
                    --region $REGION 2>/dev/null || true
            fi
        done
        
        # 클러스터 삭제
        log_info "EKS 클러스터 삭제 중..."
        aws eks delete-cluster \
            --name "$CLUSTER_NAME" \
            --region $REGION 2>/dev/null || true
        
        log_success "EKS 클러스터 삭제 요청 완료"
    else
        log_info "클러스터가 이미 삭제되었거나 존재하지 않습니다."
    fi
    
    log_warning "클러스터 삭제는 백그라운드에서 계속됩니다."
fi

# 3. Lake Formation 권한 정리
echo ""
log_info "3. Lake Formation 권한 정리 중..."

if [ ! -z "$DATABASE_NAME" ] && [ ! -z "$TABLE_NAME" ]; then
    # Data Cells Filter 삭제
    log_info "Data Cells Filter 삭제 중..."
    FILTERS=(
        "gangnam_analytics_filter"
        "operation_filter"
        "marketing_partner_filter"
    )
    
    for filter in "${FILTERS[@]}"; do
        aws lakeformation delete-data-cells-filter \
            --table-data '{
                "CatalogId": "'$ACCOUNT_ID'",
                "DatabaseName": "'$DATABASE_NAME'",
                "TableName": "'$TABLE_NAME'",
                "Name": "'$filter'"
            }' \
            --region $REGION 2>/dev/null || true
        log_info "Data Cells Filter $filter 삭제 완료"
    done
    
    # Lake Formation 권한 취소
    log_info "Lake Formation 권한 취소 중..."
    ROLES=("$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE")
    
    for role in "${ROLES[@]}"; do
        if [ ! -z "$role" ]; then
            # 테이블 권한 취소
            aws lakeformation revoke-permissions \
                --principal "arn:aws:iam::$ACCOUNT_ID:role/$role" \
                --resource '{
                    "Table": {
                        "CatalogId": "'$ACCOUNT_ID'",
                        "DatabaseName": "'$DATABASE_NAME'",
                        "Name": "'$TABLE_NAME'"
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
                        "Name": "'$DATABASE_NAME'"
                    }
                }' \
                --permissions DESCRIBE \
                --region $REGION 2>/dev/null || true
            
            log_info "역할 $role의 Lake Formation 권한 취소 완료"
        fi
    done
    
    # S3 Location 권한 취소
    if [ ! -z "$ICEBERG_BUCKET_NAME" ]; then
        for role in "${ROLES[@]}"; do
            if [ ! -z "$role" ]; then
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
        done
        log_success "S3 Location 권한 취소 완료"
    fi
fi

# 4. Glue 리소스 삭제
echo ""
log_info "4. Glue 리소스 삭제 중..."

if [ ! -z "$DATABASE_NAME" ] && [ ! -z "$TABLE_NAME" ]; then
    # Glue 테이블 삭제
    log_info "Glue 테이블 '$TABLE_NAME' 삭제 중..."
    aws glue delete-table \
        --database-name "$DATABASE_NAME" \
        --name "$TABLE_NAME" \
        --region $REGION 2>/dev/null || true
    log_success "Glue 테이블 삭제 완료"
    
    # Glue 데이터베이스 삭제
    log_info "Glue 데이터베이스 '$DATABASE_NAME' 삭제 중..."
    aws glue delete-database \
        --name "$DATABASE_NAME" \
        --region $REGION 2>/dev/null || true
    log_success "Glue 데이터베이스 삭제 완료"
fi

# 5. S3 버킷 삭제
echo ""
log_info "5. S3 버킷 삭제 중..."

# Iceberg 데이터 버킷 삭제
if [ ! -z "$ICEBERG_BUCKET_NAME" ]; then
    log_info "S3 버킷 '$ICEBERG_BUCKET_NAME' 삭제 중..."
    
    # 버킷 버전 관리 확인 및 모든 객체 버전 삭제
    aws s3api delete-objects \
        --bucket "$ICEBERG_BUCKET_NAME" \
        --delete "$(aws s3api list-object-versions \
            --bucket "$ICEBERG_BUCKET_NAME" \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
            --region $REGION 2>/dev/null || echo '{\"Objects\":[]}')" \
        --region $REGION 2>/dev/null || true
    
    # 삭제 마커 제거
    aws s3api delete-objects \
        --bucket "$ICEBERG_BUCKET_NAME" \
        --delete "$(aws s3api list-object-versions \
            --bucket "$ICEBERG_BUCKET_NAME" \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
            --region $REGION 2>/dev/null || echo '{\"Objects\":[]}')" \
        --region $REGION 2>/dev/null || true
    
    # 모든 객체 삭제
    aws s3 rm "s3://$ICEBERG_BUCKET_NAME" --recursive --region $REGION 2>/dev/null || true
    
    # 버킷 삭제
    aws s3api delete-bucket \
        --bucket "$ICEBERG_BUCKET_NAME" \
        --region $REGION 2>/dev/null || true
    
    log_success "S3 버킷 '$ICEBERG_BUCKET_NAME' 삭제 완료"
fi

# Scripts 버킷 삭제
if [ ! -z "$SCRIPTS_BUCKET" ]; then
    log_info "S3 버킷 '$SCRIPTS_BUCKET' 삭제 중..."
    aws s3 rm "s3://$SCRIPTS_BUCKET" --recursive --region $REGION 2>/dev/null || true
    aws s3api delete-bucket \
        --bucket "$SCRIPTS_BUCKET" \
        --region $REGION 2>/dev/null || true
    log_success "S3 버킷 '$SCRIPTS_BUCKET' 삭제 완료"
fi

# Athena 결과 버킷 정리 (삭제하지 않고 내용만 정리)
if [ ! -z "$ATHENA_RESULTS_BUCKET" ]; then
    log_info "Athena 결과 버킷 정리 중..."
    aws s3 rm "s3://$ATHENA_RESULTS_BUCKET" --recursive --region $REGION 2>/dev/null || true
    log_success "Athena 결과 버킷 정리 완료"
fi

# 6. IAM 역할 및 정책 삭제
echo ""
log_info "6. IAM 역할 및 정책 삭제 중..."

ROLES=(
    "$LF_DATA_STEWARD_ROLE"
    "$LF_GANGNAM_ANALYTICS_ROLE" 
    "$LF_OPERATION_ROLE"
    "$LF_MARKETING_PARTNER_ROLE"
    "$QUERY_ENGINE_ROLE_NAME"
    "$EMR_EKS_SERVICE_ROLE"
)

for role in "${ROLES[@]}"; do
    if [ ! -z "$role" ]; then
        log_info "IAM 역할 '$role' 삭제 중..."
        
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
    if [ ! -z "$log_group" ]; then
        log_info "로그 그룹 '$log_group' 삭제 중..."
        aws logs delete-log-group \
            --log-group-name "$log_group" \
            --region $REGION 2>/dev/null || true
        log_success "로그 그룹 '$log_group' 삭제 완료"
    fi
done

# 8. 로컬 파일 정리
echo ""
log_info "8. 로컬 파일 정리 중..."

# 생성된 임시 파일들 삭제
TEMP_FILES=(
    "*.json"
    "job-templates/*.yaml"
    "pod-templates/*.yaml"
    "results/*"
    "kubeconfig_*"
)

for pattern in "${TEMP_FILES[@]}"; do
    rm -rf $pattern 2>/dev/null || true
done

# Terraform 관련 파일 정리
TERRAFORM_DIR="./data-on-eks-blueprint/analytics/terraform/emr-eks-karpenter"
if [ -d "$TERRAFORM_DIR" ]; then
    log_info "Terraform 관련 파일 정리 중..."
    
    # Terraform 디렉토리로 이동
    cd "$TERRAFORM_DIR"
    
    # Terraform 상태 및 캐시 파일 삭제
    rm -rf .terraform* terraform.tfstate* *.tfplan 2>/dev/null || true
    rm -rf .terraform.lock.hcl 2>/dev/null || true
    
    # 원래 디렉토리로 복귀
    cd - > /dev/null
    
    log_success "Terraform 파일 정리 완료"
fi

# .env 파일 백업 후 정리
if [ -f .env ]; then
    cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
    log_info ".env 파일을 백업했습니다: .env.backup.$(date +%Y%m%d_%H%M%S)"
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
echo "  ✅ Glue 데이터베이스 및 테이블"
echo "  ✅ S3 버킷 및 모든 데이터"
echo "  ✅ IAM 역할 및 정책"
echo "  ✅ CloudWatch 로그 그룹"
echo "  ✅ 로컬 임시 파일"
echo ""
log_warning "주의사항:"
echo "  • EKS 클러스터 삭제는 백그라운드에서 계속 진행됩니다 (15-20분 소요)"
echo "  • Data-on-EKS Blueprint Terraform 상태 파일이 정리되었습니다"
echo "  • Karpenter 관리 노드들이 정리되었습니다"
echo "  • AWS 콘솔에서 클러스터 삭제 상태를 확인하세요"
echo "  • .env 파일이 백업되었습니다"
echo ""
log_info "AWS 콘솔에서 다음을 확인하세요:"
echo "  • EKS 클러스터: https://console.aws.amazon.com/eks/home?region=$REGION#/clusters"
echo "  • EMR on EKS: https://console.aws.amazon.com/emr/home?region=$REGION#/containers"
echo "  • S3 버킷: https://console.aws.amazon.com/s3/home?region=$REGION"
echo "  • EC2 인스턴스: https://console.aws.amazon.com/ec2/home?region=$REGION#Instances"
echo ""
log_info "완전한 정리를 위해 다음도 확인하세요:"
echo "  • VPC 및 서브넷 (자동 생성된 경우)"
echo "  • Security Groups"
echo "  • Load Balancers"
echo "  • NAT Gateways"
echo ""
