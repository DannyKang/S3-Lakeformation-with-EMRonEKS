#!/bin/bash

# EMR on EKS Blueprint 기반 클러스터 설정 스크립트
# 참조: https://awslabs.github.io/data-on-eks/docs/blueprints/amazon-emr-on-eks/emr-eks-karpenter
# Lake Formation FGAC와 S3 Tables 통합 지원
# Prometheus, Kubecost, Proportional autoscaler 제외

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
    echo "파일 내용을 확인하거나 01-create-s3-table-bucket.sh를 다시 실행하세요."
    exit 1
fi

# 필수 환경 변수 확인
if [ -z "$ACCOUNT_ID" ] || [ -z "$REGION" ] || [ -z "$ICEBERG_BUCKET_NAME" ] || [ -z "$LF_DATA_STEWARD_ROLE" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "이전 단계들을 순서대로 다시 실행하세요."
    exit 1
fi

# Blueprint 기반 설정
CLUSTER_NAME="seoul-bike-emr"
NAMESPACE="emr-data-team"
VIRTUAL_CLUSTER_NAME="seoul-bike-emr-vc"
KARPENTER_VERSION="1.6.0"
TERRAFORM_VERSION="1.9.8"

echo "=== EMR on EKS Blueprint 클러스터 설정 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "S3 Iceberg 버킷: $ICEBERG_BUCKET_NAME"
echo "EKS 클러스터: $CLUSTER_NAME"
echo "네임스페이스: $NAMESPACE"
echo "Karpenter 버전: $KARPENTER_VERSION"
echo ""

# 1. 필수 도구 확인 및 설치
echo "1. 필수 도구 확인 및 설치..."

# OS 감지
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
    ARCH="amd64"
    if [[ $(uname -m) == "arm64" ]]; then
        ARCH="arm64"
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
    ARCH="amd64"
else
    echo "   ❌ 지원되지 않는 OS: $OSTYPE"
    exit 1
fi

echo "   감지된 OS: $OS ($ARCH)"

# kubectl 설치
if command -v kubectl >/dev/null 2>&1; then
    echo "   ✅ kubectl 설치됨"
else
    echo "   kubectl 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        curl -LO "https://dl.k8s.io/release/v1.31.0/bin/darwin/${ARCH}/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/
    else
        curl -LO "https://dl.k8s.io/release/v1.31.0/bin/linux/${ARCH}/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/
    fi
    echo "   ✅ kubectl 설치 완료"
fi

# helm 설치
if command -v helm >/dev/null 2>&1; then
    echo "   ✅ helm 설치됨"
else
    echo "   helm 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install helm
        else
            curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
        fi
    else
        curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    fi
    echo "   ✅ helm 설치 완료"
fi

# aws cli 설치
if command -v aws >/dev/null 2>&1; then
    echo "   ✅ aws cli 설치됨"
else
    echo "   aws cli 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install awscli
        else
            curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
            sudo installer -pkg AWSCLIV2.pkg -target /
            rm AWSCLIV2.pkg
        fi
    else
        curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
        unzip awscliv2.zip
        sudo ./aws/install
        rm -rf aws awscliv2.zip
    fi
    echo "   ✅ aws cli 설치 완료"
fi

# jq 설치
if command -v jq >/dev/null 2>&1; then
    echo "   ✅ jq 설치됨"
else
    echo "   jq 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install jq
        else
            curl -L "https://github.com/stedolan/jq/releases/latest/download/jq-osx-amd64" -o jq
            chmod +x jq
            sudo mv jq /usr/local/bin/
        fi
    else
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update && sudo apt-get install -y jq
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y jq
        else
            curl -L "https://github.com/stedolan/jq/releases/latest/download/jq-linux64" -o jq
            chmod +x jq
            sudo mv jq /usr/local/bin/
        fi
    fi
    echo "   ✅ jq 설치 완료"
fi

# eksctl 확인 및 설치
if command -v eksctl >/dev/null 2>&1; then
    EKSCTL_VERSION=$(eksctl version)
    echo "   ✅ eksctl 설치됨: $EKSCTL_VERSION"
else
    echo "   eksctl 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        if [[ "$ARCH" == "arm64" ]]; then
            curl --silent --location "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Darwin_arm64.tar.gz" | tar xz -C /tmp
        else
            curl --silent --location "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Darwin_amd64.tar.gz" | tar xz -C /tmp
        fi
    else
        curl --silent --location "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Linux_amd64.tar.gz" | tar xz -C /tmp
    fi
    sudo mv /tmp/eksctl /usr/local/bin
    echo "   ✅ eksctl 설치 완료"
fi

# Terraform 확인 및 설치
if command -v terraform >/dev/null 2>&1; then
    TF_VERSION=$(terraform version -json | jq -r '.terraform_version')
    echo "   ✅ Terraform 설치됨: v$TF_VERSION"
else
    echo "   Terraform 설치 중..."
    if [[ "$OS" == "macos" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install terraform
        else
            if [[ "$ARCH" == "arm64" ]]; then
                TF_ARCH="darwin_arm64"
            else
                TF_ARCH="darwin_amd64"
            fi
            wget https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_${TF_ARCH}.zip
            unzip terraform_${TERRAFORM_VERSION}_${TF_ARCH}.zip
            sudo mv terraform /usr/local/bin/
            rm terraform_${TERRAFORM_VERSION}_${TF_ARCH}.zip
        fi
    else
        wget https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip
        unzip terraform_${TERRAFORM_VERSION}_linux_amd64.zip
        sudo mv terraform /usr/local/bin/
        rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip
    fi
    echo "   ✅ Terraform 설치 완료"
fi

# 2. Data on EKS Blueprint 준비 및 Terraform 설정 확인
echo ""
echo "2. Data on EKS Blueprint 준비..."

BLUEPRINT_DIR="data-on-eks-blueprint"
BLUEPRINT_PATH="$BLUEPRINT_DIR/analytics/terraform/emr-eks-karpenter"
SKIP_TERRAFORM_SETUP=false

# 기존 Blueprint와 Terraform 상태 확인
if [ -d "$BLUEPRINT_DIR" ] && [ -d "$BLUEPRINT_PATH" ] && [ -f "$BLUEPRINT_PATH/terraform.tfstate" ]; then
    echo "   ✅ 기존 Data on EKS Blueprint 발견 - Terraform 초기 설정을 건너뜁니다"
    SKIP_TERRAFORM_SETUP=true
    
    # 기존 Terraform 출력값 가져오기
    cd $BLUEPRINT_PATH
    if terraform output aws_region >/dev/null 2>&1; then
        # 실제 존재하는 output 변수들 사용
        AWS_REGION_OUTPUT=$(terraform output -raw aws_region 2>/dev/null || echo "$REGION")
        CONFIGURE_KUBECTL=$(terraform output -raw configure_kubectl 2>/dev/null || echo "")
        EMR_S3_BUCKET_NAME=$(terraform output -raw emr_s3_bucket_name 2>/dev/null || echo "")
        GRAFANA_SECRET_NAME=$(terraform output -raw grafana_secret_name 2>/dev/null || echo "")
        
        # 클러스터 이름은 configure_kubectl에서 추출하거나 기본값 사용
        if [ ! -z "$CONFIGURE_KUBECTL" ]; then
            CLUSTER_NAME_OUTPUT=$(echo "$CONFIGURE_KUBECTL" | grep -o 'update-kubeconfig --name [^ ]*' | cut -d' ' -f3)
        else
            CLUSTER_NAME_OUTPUT=$CLUSTER_NAME
        fi
        
        # EMR Virtual Cluster ID 추출 (data-team-a에서)
        VIRTUAL_CLUSTER_ID=$(terraform output -json emr_on_eks 2>/dev/null | jq -r '.["data-team-a"].virtual_cluster_id' 2>/dev/null || echo "")
        
        echo "   기존 클러스터 정보:"
        echo "   • 클러스터 이름: $CLUSTER_NAME_OUTPUT"
        echo "   • AWS 리전: $AWS_REGION_OUTPUT"
        [ ! -z "$EMR_S3_BUCKET_NAME" ] && echo "   • EMR S3 버킷: $EMR_S3_BUCKET_NAME"
        [ ! -z "$VIRTUAL_CLUSTER_ID" ] && echo "   • Virtual Cluster ID: $VIRTUAL_CLUSTER_ID"
        
        # EKS 클러스터에서 추가 정보 가져오기
        CLUSTER_INFO=$(aws eks describe-cluster --name $CLUSTER_NAME_OUTPUT --region $REGION 2>/dev/null || echo "")
        if [ ! -z "$CLUSTER_INFO" ]; then
            CLUSTER_ENDPOINT=$(echo "$CLUSTER_INFO" | jq -r '.cluster.endpoint' 2>/dev/null || echo "")
            OIDC_ISSUER=$(echo "$CLUSTER_INFO" | jq -r '.cluster.identity.oidc.issuer' 2>/dev/null || echo "")
            if [ ! -z "$OIDC_ISSUER" ]; then
                OIDC_ID=$(echo "$OIDC_ISSUER" | sed 's|https://oidc.eks.[^/]*/id/||')
                OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/$(echo "$OIDC_ISSUER" | sed 's|https://||')"
            fi
            VPC_ID=$(echo "$CLUSTER_INFO" | jq -r '.cluster.resourcesVpcConfig.vpcId' 2>/dev/null || echo "")
            CLUSTER_SECURITY_GROUP_ID=$(echo "$CLUSTER_INFO" | jq -r '.cluster.resourcesVpcConfig.clusterSecurityGroupId' 2>/dev/null || echo "")
        fi
    else
        echo "   ⚠️  Terraform 출력값을 가져올 수 없습니다. 새로 배포를 진행합니다."
        SKIP_TERRAFORM_SETUP=false
    fi
    cd - >/dev/null
fi

# Terraform 초기 설정이 필요한 경우에만 실행
if [ "$SKIP_TERRAFORM_SETUP" = false ]; then
    if [ ! -d "$BLUEPRINT_DIR" ]; then
        echo "   Data on EKS Blueprint 클론 중..."
        git clone https://github.com/awslabs/data-on-eks.git $BLUEPRINT_DIR
    else
        echo "   ✅ Data on EKS Blueprint 디렉토리 존재"
    fi

    # EMR on EKS Karpenter Blueprint 디렉토리 확인
    if [ ! -d "$BLUEPRINT_PATH" ]; then
        echo "   ❌ EMR on EKS Karpenter Blueprint를 찾을 수 없습니다."
        echo "   Blueprint 구조가 변경되었을 수 있습니다."
        exit 1
    fi

    echo "   ✅ EMR on EKS Karpenter Blueprint 준비 완료"

    # 3. Terraform 변수 파일 생성
    echo ""
    echo "3. Terraform 변수 파일 생성..."

    # Blueprint용 terraform.tfvars 생성
    cat > $BLUEPRINT_PATH/terraform.tfvars << EOF
# AWS 기본 설정
region = "$REGION"

# EKS 클러스터 설정
name = "$CLUSTER_NAME"
cluster_version = "1.31"

# VPC 설정
vpc_cidr = "10.1.0.0/16"
azs      = ["${REGION}a", "${REGION}b", "${REGION}c"]

# EKS 관리형 노드 그룹 설정
enable_managed_nodegroups = true
managed_node_groups = {
  mg_5 = {
    node_group_name = "managed-ondemand"
    instance_types  = ["m5.large", "m5.xlarge"]
    min_size        = 2
    max_size        = 10
    desired_size    = 3
    subnet_ids      = [] # Will be populated by module
  }
}

# Karpenter 설정
enable_karpenter = true
karpenter = {
  chart_version = "$KARPENTER_VERSION"
  repository    = "oci://public.ecr.aws/karpenter"
  namespace     = "kube-system"
}

# EMR on EKS 설정
enable_emr_on_eks = true
emr_on_eks_teams = {
  data_team = {
    namespace               = "$NAMESPACE"
    job_execution_role      = "EMRContainers-JobExecutionRole"
    additional_iam_policies = []
  }
}

# Lake Formation 통합을 위한 추가 설정
enable_aws_load_balancer_controller = true
enable_cluster_autoscaler           = false  # Karpenter 사용으로 비활성화
enable_metrics_server              = true
enable_cluster_proportional_autoscaler = false  # 요구사항에 따라 비활성화

# 모니터링 도구 비활성화 (요구사항에 따라)
enable_amazon_prometheus          = false
enable_kube_prometheus_stack      = false
enable_prometheus                 = false
enable_kubecost                   = false


# 추가 태그
tags = {
  Blueprint  = "emr-eks-karpenter"
  Project    = "seoul-bike-analytics"
  Purpose    = "lake-formation-fgac-demo"
}
EOF

    echo "   ✅ Terraform 변수 파일 생성 완료: $BLUEPRINT_PATH/terraform.tfvars"
    
    # 4. Terraform 초기화 및 배포
    echo ""
    echo "4. Terraform을 사용한 EKS 클러스터 배포..."

    cd $BLUEPRINT_PATH

    # Terraform 초기화
    echo "   Terraform 초기화 중..."
    terraform init -upgrade >/dev/null 2>&1

    # Terraform 계획 확인
    echo "   Terraform 계획 생성 중..."
    terraform plan -out=tfplan >/dev/null 2>&1

    # 사용자 확인
    echo ""
    echo "   📋 배포될 리소스:"
    echo "   • EKS 클러스터: $CLUSTER_NAME"
    echo "   • VPC 및 서브넷"
    echo "   • EKS 관리형 노드 그룹"
    echo "   • Karpenter $KARPENTER_VERSION"
    echo "   • EMR on EKS Virtual Cluster"
    echo "   • AWS Load Balancer Controller"
    echo "   • Metrics Server"
    echo ""
    echo "   ⚠️  이 작업은 약 15-20분이 소요되며 AWS 비용이 발생합니다."
    echo ""
    read -p "   계속 진행하시겠습니까? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "   배포가 취소되었습니다."
        exit 1
    fi

    # Terraform 적용
    echo "   Terraform 적용 중... (약 15-20분 소요)"
    terraform apply tfplan

    if [ $? -eq 0 ]; then
        echo "   ✅ EKS 클러스터 배포 완료"
        
        # kubectl 컨텍스트 설정
        echo "   kubectl 컨텍스트 설정 중..."
        aws eks update-kubeconfig --region $REGION --name $CLUSTER_NAME
        
        # 현재 사용자를 cluster-admin으로 추가
        echo "   현재 사용자에게 cluster-admin 권한 부여 중..."
        CURRENT_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
        
        # aws-auth ConfigMap 업데이트
        kubectl patch configmap/aws-auth -n kube-system --type merge -p '{
          "data": {
            "mapUsers": "[{\"userarn\":\"'$CURRENT_USER_ARN'\",\"username\":\"cluster-admin\",\"groups\":[\"system:masters\"]}]"
          }
        }' || echo "   ⚠️  aws-auth 업데이트 실패 (계속 진행)"
        
        # 잠시 대기 후 재시도
        echo "   권한 적용 대기 중..."
        sleep 10
        
        # Terraform 재적용 (권한 문제 해결 후)
        echo "   권한 문제 해결 후 Terraform 재적용 중..."
        terraform apply -auto-approve
        
    else
        echo "   ❌ EKS 클러스터 배포 실패"
        exit 1
    fi

    # Terraform 출력값 가져오기
    AWS_REGION_OUTPUT=$(terraform output -raw aws_region 2>/dev/null || echo "$REGION")
    CONFIGURE_KUBECTL=$(terraform output -raw configure_kubectl 2>/dev/null || echo "")
    EMR_S3_BUCKET_NAME=$(terraform output -raw emr_s3_bucket_name 2>/dev/null || echo "")
    
    # 클러스터 이름은 configure_kubectl에서 추출
    if [ ! -z "$CONFIGURE_KUBECTL" ]; then
        CLUSTER_NAME_OUTPUT=$(echo "$CONFIGURE_KUBECTL" | grep -o 'update-kubeconfig --name [^ ]*' | cut -d' ' -f3)
    else
        CLUSTER_NAME_OUTPUT=$CLUSTER_NAME
    fi
    
    # EMR Virtual Cluster ID 추출 (data-team-a에서)
    VIRTUAL_CLUSTER_ID=$(terraform output -json emr_on_eks 2>/dev/null | jq -r '.["data-team-a"].virtual_cluster_id' 2>/dev/null || echo "")
    
    # EKS 클러스터에서 추가 정보 가져오기
    CLUSTER_INFO=$(aws eks describe-cluster --name $CLUSTER_NAME_OUTPUT --region $REGION 2>/dev/null || echo "")
    if [ ! -z "$CLUSTER_INFO" ]; then
        CLUSTER_ENDPOINT=$(echo "$CLUSTER_INFO" | jq -r '.cluster.endpoint' 2>/dev/null || echo "")
        OIDC_ISSUER=$(echo "$CLUSTER_INFO" | jq -r '.cluster.identity.oidc.issuer' 2>/dev/null || echo "")
        if [ ! -z "$OIDC_ISSUER" ]; then
            OIDC_ID=$(echo "$OIDC_ISSUER" | sed 's|https://oidc.eks.[^/]*/id/||')
            OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/$(echo "$OIDC_ISSUER" | sed 's|https://||')"
        fi
        VPC_ID=$(echo "$CLUSTER_INFO" | jq -r '.cluster.resourcesVpcConfig.vpcId' 2>/dev/null || echo "")
        CLUSTER_SECURITY_GROUP_ID=$(echo "$CLUSTER_INFO" | jq -r '.cluster.resourcesVpcConfig.clusterSecurityGroupId' 2>/dev/null || echo "")
    fi

    echo "   클러스터 이름: $CLUSTER_NAME_OUTPUT"
    echo "   클러스터 엔드포인트: $CLUSTER_ENDPOINT"
    echo "   OIDC Provider ARN: $OIDC_PROVIDER_ARN"
    [ ! -z "$VIRTUAL_CLUSTER_ID" ] && echo "   Virtual Cluster ID: $VIRTUAL_CLUSTER_ID"

    # 원래 디렉토리로 돌아가기
    cd - >/dev/null
else
    echo ""
    echo "3-4. Terraform 초기 설정 건너뜀 (기존 인프라 사용)"
    echo "   기존 EKS 클러스터를 사용하여 Lake Formation 설정을 진행합니다."
    
    # 기존 클러스터 정보 재확인
    if [ -z "$CLUSTER_NAME_OUTPUT" ]; then
        CLUSTER_NAME_OUTPUT=$CLUSTER_NAME
    fi
fi

# 5. kubectl 컨텍스트 설정
echo ""
echo "5. kubectl 컨텍스트 설정..."
aws eks update-kubeconfig --region $REGION --name $CLUSTER_NAME_OUTPUT
echo "   ✅ kubectl 컨텍스트 설정 완료"

# 6. 클러스터 상태 확인
echo ""
echo "6. 클러스터 상태 확인..."

# 노드 상태 확인
echo "   노드 상태 확인 중..."
kubectl get nodes -o wide

# Karpenter 상태 확인
echo "   Karpenter 상태 확인 중..."
kubectl get pods -n kube-system -l app.kubernetes.io/name=karpenter

# EMR on EKS 네임스페이스 확인
echo "   EMR 네임스페이스 확인 중..."
# 실제 존재하는 EMR 네임스페이스 찾기
EMR_NAMESPACES=$(kubectl get namespaces -o name | grep -E "emr-data-team" | head -1 | cut -d'/' -f2)
if [ ! -z "$EMR_NAMESPACES" ]; then
    echo "   ✅ EMR 네임스페이스 발견: $EMR_NAMESPACES"
    # 첫 번째 발견된 네임스페이스를 기본으로 사용
    NAMESPACE=$(kubectl get namespaces -o name | grep -E "emr-data-team" | head -1 | cut -d'/' -f2)
else
    # 기본 네임스페이스가 없으면 생성
    echo "   ⚠️  EMR 네임스페이스가 없습니다. $NAMESPACE 생성 중..."
    kubectl create namespace $NAMESPACE || echo "   네임스페이스가 이미 존재합니다."
fi

echo "   ✅ 클러스터 상태 확인 완료"

# 7. Lake Formation 통합을 위한 추가 설정
echo ""
echo "7. Lake Formation 통합을 위한 추가 설정..."

# EMR Virtual Cluster ID 가져오기 (Terraform output에서 먼저 시도)
if [ -z "$VIRTUAL_CLUSTER_ID" ]; then
    VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters \
        --region $REGION \
        --query "virtualClusters[?name=='$VIRTUAL_CLUSTER_NAME' && state=='RUNNING'].id" \
        --output text)
fi

if [ -z "$VIRTUAL_CLUSTER_ID" ] || [ "$VIRTUAL_CLUSTER_ID" = "None" ]; then
    echo "   ⚠️  EMR Virtual Cluster를 찾을 수 없습니다. 수동으로 생성이 필요할 수 있습니다."
    # Terraform output에서 data-team-a의 virtual cluster id 사용
    cd $BLUEPRINT_PATH 2>/dev/null || true
    VIRTUAL_CLUSTER_ID=$(terraform output -json emr_on_eks 2>/dev/null | jq -r '.["data-team-a"].virtual_cluster_id' 2>/dev/null || echo "")
    cd - >/dev/null 2>&1 || true
    
    if [ ! -z "$VIRTUAL_CLUSTER_ID" ] && [ "$VIRTUAL_CLUSTER_ID" != "null" ]; then
        echo "   ✅ Terraform에서 EMR Virtual Cluster ID 발견: $VIRTUAL_CLUSTER_ID"
    else
        VIRTUAL_CLUSTER_ID=""
    fi
else
    echo "   ✅ EMR Virtual Cluster ID: $VIRTUAL_CLUSTER_ID"
fi

# 8. Lake Formation IAM 역할 확인 및 IRSA 설정
echo ""
echo "8. Lake Formation IAM 역할 IRSA 설정..."

# Lake Formation 역할 확인
ROLES=("$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE")

for role in "${ROLES[@]}"; do
    if aws iam get-role --role-name $role >/dev/null 2>&1; then
        echo "   ✅ $role 존재 확인"
    else
        echo "   ❌ $role이 존재하지 않습니다."
        echo "      먼저 ./scripts/02-create-iam-roles.sh를 실행하세요."
        exit 1
    fi
done

# OIDC 정보 추출 (기존 클러스터 사용 시 다시 가져오기)
if [ -z "$OIDC_PROVIDER_ARN" ]; then
    OIDC_PROVIDER_ARN=$(aws eks describe-cluster --name $CLUSTER_NAME_OUTPUT --region $REGION --query 'cluster.identity.oidc.issuer' --output text | sed 's|https://||')
    OIDC_PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_PROVIDER_ARN}"
fi

OIDC_ID=$(echo $OIDC_PROVIDER_ARN | cut -d'/' -f4)

# Lake Formation 역할별 서비스 계정 생성 및 IRSA 설정
SERVICE_ACCOUNTS=(
    "emr-data-steward-sa:$LF_DATA_STEWARD_ROLE"
    "emr-gangnam-analytics-sa:$LF_GANGNAM_ANALYTICS_ROLE"
    "emr-operation-sa:$LF_OPERATION_ROLE"
    "emr-marketing-partner-sa:$LF_MARKETING_PARTNER_ROLE"
)

# 실제 EMR 네임스페이스 사용 (첫 번째 발견된 것)
IRSA_NAMESPACE=$(kubectl get namespaces -o name | grep -E "emr-data-team" | head -1 | cut -d'/' -f2)
if [ -z "$IRSA_NAMESPACE" ]; then
    IRSA_NAMESPACE=$NAMESPACE
fi

echo "   IRSA 설정에 사용할 네임스페이스: $IRSA_NAMESPACE"

for sa_info in "${SERVICE_ACCOUNTS[@]}"; do
    IFS=':' read -r sa_name role_name <<< "$sa_info"
    
    echo "   $sa_name IRSA 설정 중..."
    
    # 서비스 계정 생성 (IRSA 사용)
    eksctl create iamserviceaccount \
        --cluster=$CLUSTER_NAME_OUTPUT \
        --region=$REGION \
        --name=$sa_name \
        --namespace=$IRSA_NAMESPACE \
        --attach-role-arn=arn:aws:iam::${ACCOUNT_ID}:role/$role_name \
        --approve \
        --override-existing-serviceaccounts >/dev/null 2>&1 || echo "     서비스 계정이 이미 존재합니다."
    
    echo "   ✅ $sa_name IRSA 설정 완료"
done

# 9. Spark 코드 및 설정 S3 업로드
echo ""
echo "9. Spark 코드 S3 업로드..."

SCRIPTS_BUCKET="seoul-bike-analytics-scripts-${ACCOUNT_ID}"

# S3 버킷 생성
aws s3 mb s3://$SCRIPTS_BUCKET --region $REGION 2>/dev/null || echo "   버킷이 이미 존재합니다."

# Spark 코드 업로드 (기존 코드가 있다면)
if [ -d "spark-jobs" ]; then
    aws s3 sync spark-jobs/ s3://$SCRIPTS_BUCKET/spark-jobs/
    echo "   ✅ Spark 코드 업로드 완료: s3://$SCRIPTS_BUCKET/spark-jobs/"
else
    echo "   ⚠️  spark-jobs 디렉토리가 없습니다. 나중에 업로드하세요."
fi

# 10. 환경 변수 파일 업데이트
echo ""
echo "10. 환경 변수 파일 업데이트..."

# Blueprint 기반 설정을 .env 파일에 추가
cat >> .env << EOF

# EMR on EKS Blueprint 설정 ($(date '+%Y-%m-%d %H:%M:%S'))
CLUSTER_NAME=$CLUSTER_NAME_OUTPUT
EMR_NAMESPACE=$NAMESPACE
VIRTUAL_CLUSTER_ID=$VIRTUAL_CLUSTER_ID
VIRTUAL_CLUSTER_NAME=$VIRTUAL_CLUSTER_NAME
SCRIPTS_BUCKET=$SCRIPTS_BUCKET

# Blueprint 정보
BLUEPRINT_TYPE=data-on-eks-emr-karpenter
BLUEPRINT_VERSION=latest
TERRAFORM_VERSION=$TERRAFORM_VERSION

# 클러스터 정보
CLUSTER_ENDPOINT=$CLUSTER_ENDPOINT
CLUSTER_SECURITY_GROUP_ID=$CLUSTER_SECURITY_GROUP_ID
VPC_ID=$VPC_ID

# IRSA 설정 정보
IRSA_ENABLED=true
OIDC_PROVIDER_ARN=$OIDC_PROVIDER_ARN
OIDC_ID=$OIDC_ID

# Karpenter 설정
KARPENTER_VERSION=$KARPENTER_VERSION
KARPENTER_NAMESPACE=kube-system

# 추가 애드온
AWS_LOAD_BALANCER_CONTROLLER=enabled
METRICS_SERVER=enabled
CLUSTER_AUTOSCALER=disabled
PROMETHEUS=disabled
GRAFANA=disabled
KUBECOST=disabled
EOF

echo "   ✅ 환경 변수 파일 업데이트 완료: .env"

# 11. 클러스터 검증
echo ""
echo "11. 클러스터 검증..."

# 노드 상태 재확인
echo "   노드 상태:"
kubectl get nodes --show-labels | grep -E "NAME|Ready"

# EMR 네임스페이스의 서비스 계정 확인
echo "   EMR 서비스 계정 상태:"
# 모든 EMR 네임스페이스에서 서비스 계정 확인
for ns in $(kubectl get namespaces -o name | grep -E "emr-data-team" | cut -d'/' -f2); do
    echo "   네임스페이스 $ns:"
    kubectl get serviceaccounts -n $ns 2>/dev/null || echo "     서비스 계정이 없습니다."
done

echo "   ✅ 클러스터 검증 완료"

echo ""
echo "=== EMR on EKS Blueprint 클러스터 설정 완료 ==="
echo ""
echo "📋 설정된 리소스 요약:"
echo "┌─────────────────────────────┬─────────────────────────────────────┐"
echo "│ 리소스                      │ 값                                  │"
echo "├─────────────────────────────┼─────────────────────────────────────┤"
echo "│ EKS 클러스터                │ $CLUSTER_NAME_OUTPUT                │"
echo "│ EMR 네임스페이스            │ $NAMESPACE                          │"
echo "│ Virtual Cluster ID          │ $VIRTUAL_CLUSTER_ID                 │"
echo "│ Spark 코드 버킷             │ s3://$SCRIPTS_BUCKET                │"
echo "│ VPC ID                      │ $VPC_ID                             │"
echo "│ OIDC Provider               │ $OIDC_ID                            │"
echo "└─────────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "🎭 Lake Formation 역할 연결 (IRSA):"
echo "   • emr-data-steward-sa → LF_DataStewardRole"
echo "   • emr-gangnam-analytics-sa → LF_GangnamAnalyticsRole"
echo "   • emr-operation-sa → LF_OperationRole"
echo "   • emr-marketing-partner-sa → LF_MarketingPartnerRole"
echo ""
echo "✅ 다음 단계: ./scripts/06-setup-lake-formation-fgac.sh"
echo ""
echo "⚠️  주의사항:"
if [ "$SKIP_TERRAFORM_SETUP" = false ]; then
    echo "   • 이 클러스터는 Terraform으로 관리됩니다"
    echo "   • 삭제 시: cd $BLUEPRINT_PATH && terraform destroy"
    echo "   • 수정 시: terraform.tfvars 파일을 편집 후 terraform apply"
else
    echo "   • 기존 클러스터를 사용했습니다"
    echo "   • Terraform 상태는 $BLUEPRINT_PATH 에 있습니다"
fi
echo ""
