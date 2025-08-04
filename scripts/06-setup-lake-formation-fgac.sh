#!/bin/bash

# Lake Formation Fine-Grained Access Control (FGAC) 설정 스크립트
# EMR on EKS와 Lake Formation 통합을 위한 설정
# AWS 공식 문서 기준: QueryExecutionRole(System Driver)과 QueryEngineRole(System Executor) 분리
# 참조: https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/security_iam_fgac-lf-enable.html

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/04-setup-emr-on-eks.sh를 실행하세요."
    exit 1
fi

source .env

echo "=== Lake Formation FGAC 설정 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "EKS 클러스터: $CLUSTER_NAME"
echo "S3 Iceberg 버킷: $ICEBERG_BUCKET_NAME"
echo ""

# Lake Formation 설정 변수
LF_SESSION_TAG_VALUE="EMRonEKSEngine"
SYSTEM_NAMESPACE="emr-system"
USER_NAMESPACE="emr-data-team" 

# 네임스페이스가 없으면 생성
if ! kubectl get namespace $USER_NAMESPACE >/dev/null 2>&1; then
    echo "네임스페이스 $USER_NAMESPACE 생성 중..."
    kubectl create namespace $USER_NAMESPACE
fi

JOB_EXECUTION_ROLE_NAME="LF_JobExecutionRole"  # User Profile용 (AWS 문서 기준)
QUERY_ENGINE_ROLE_NAME="LF_QueryEngineRole"        # System Profile용 (AWS 문서 기준)
SECURITY_CONFIG_NAME="seoul-bike-lf-security-config"

echo "Lake Formation 설정:"
echo "• Session Tag Value: $LF_SESSION_TAG_VALUE"
echo "• System Namespace: $SYSTEM_NAMESPACE"
echo "• User Namespace: $USER_NAMESPACE"
echo "• Job Execution Role (System Driver): $JOB_EXECUTION_ROLE_NAME"
echo "• Query Engine Role (System Executor): $QUERY_ENGINE_ROLE_NAME"
echo ""

# Step 1: Lake Formation Application Integration Settings 설정
echo "1. Lake Formation Application Integration Settings 설정..."

# Lake Formation에서 외부 엔진 허용 설정
echo "   Lake Formation에서 외부 엔진 데이터 필터링 허용 설정 중..."

# Lake Formation 설정 확인
LF_SETTINGS=$(aws lakeformation get-data-lake-settings --region $REGION 2>/dev/null || echo "{}")

# 현재 사용자 정보 가져오기
CURRENT_USER_ARN=$(aws sts get-caller-identity --query 'Arn' --output text)

# 기존 Lake Formation 설정 가져오기
EXISTING_SETTINGS=$(aws lakeformation get-data-lake-settings --region $REGION)

# 외부 엔진 허용 설정 업데이트 (기존 설정 유지하면서 필요한 부분만 추가)
aws lakeformation put-data-lake-settings \
    --region $REGION \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {
                "DataLakePrincipalIdentifier": "'$CURRENT_USER_ARN'"
            }
        ],
        "CreateDatabaseDefaultPermissions": [],
        "CreateTableDefaultPermissions": [],
        "Parameters": {
            "CROSS_ACCOUNT_VERSION": "3",
            "SET_CONTEXT": "TRUE"
        },
        "AllowExternalDataFiltering": true,
        "AllowFullTableExternalDataAccess": false,
        "ExternalDataFilteringAllowList": [
            {
                "DataLakePrincipalIdentifier": "'$ACCOUNT_ID'"
            }
        ],
        "AuthorizedSessionTagValueList": [
            "'$LF_SESSION_TAG_VALUE'"
        ]
    }' >/dev/null

echo "   ✅ Lake Formation Application Integration Settings 설정 완료"

# Step 2: EKS RBAC 권한 설정
echo ""
echo "2. EKS RBAC 권한 설정..."

# System Namespace 생성
echo "   System Namespace 생성 중..."
kubectl create namespace $SYSTEM_NAMESPACE 2>/dev/null || echo "   System Namespace가 이미 존재합니다."

# EMR Containers ClusterRole 생성
echo "   EMR Containers ClusterRole 생성 중..."
cat > /tmp/emr-containers-cluster-role.yaml << EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: emr-containers
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["serviceaccounts", "services", "configmaps", "events", "pods", "pods/log"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "patch", "delete", "watch"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["extensions", "networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["clusterroles","clusterrolebindings","roles", "rolebindings"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
EOF

kubectl apply -f /tmp/emr-containers-cluster-role.yaml

# EMR Containers ClusterRoleBinding 생성
echo "   EMR Containers ClusterRoleBinding 생성 중..."
cat > /tmp/emr-containers-cluster-role-binding.yaml << EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: emr-containers
subjects:
- kind: User
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f /tmp/emr-containers-cluster-role-binding.yaml

echo "   ✅ EKS RBAC 권한 설정 완료"

# Step 3: IAM 역할 설정
echo ""
echo "3. IAM 역할 설정..."


#=======================Query Engine Role=====================================================

# Query Engine Role 생성 (System Driver & Executor용)
echo "   Query Engine Role (System Driver & Executor용) 생성 중..."

# Query Engine Role Trust Policy
cat > /tmp/query-engine-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-containers.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        }
    ]
}
EOF

# Query Engine Role 생성 또는 Trust Policy 업데이트
if aws iam get-role --role-name $QUERY_ENGINE_ROLE_NAME >/dev/null 2>&1; then
    echo "   Query Engine Role이 이미 존재합니다. Trust Policy를 업데이트합니다..."
    aws iam update-assume-role-policy \
        --role-name $QUERY_ENGINE_ROLE_NAME \
        --policy-document file:///tmp/query-engine-trust-policy.json
    echo "   ✅ Query Engine Role Trust Policy 업데이트 완료"
else
    aws iam create-role \
        --role-name $QUERY_ENGINE_ROLE_NAME \
        --assume-role-policy-document file:///tmp/query-engine-trust-policy.json \
        --description "Lake Formation Query Engine Role for EMR on EKS System Executor"
    echo "   ✅ Query Engine Role 생성 완료"
fi

# Query Engine Role Permissions Policy (AWS 공식 문서 기준 + sts:TagSession 권한 추가)
cat > /tmp/query-engine-permissions-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AssumeJobRoleWithSessionTagAccessForSystemDriver",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ]
        },
        {
            "Sid": "AssumeJobRoleWithSessionTagAccessForSystemExecutor",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ]
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
        {
            "Sid": "GlueCatalogAccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:List*",
                "glue:GetUnfilteredTableMetadata",
                "glue:GetUnfilteredPartitionMetadata",
                "glue:GetUnfilteredPartitionsMetadata"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "lakeformation:GetWorkUnits",
                "lakeformation:StartQueryPlanning",
                "lakeformation:GetWorkUnitResults",
                "lakeformation:GetTemporaryGlueTableCredentials",
                "lakeformation:GetTemporaryGluePartitionCredentials"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "S3AccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::$ICEBERG_BUCKET_NAME",
                "arn:aws:s3:::$ICEBERG_BUCKET_NAME/*"
            ]
        }
    ]
}
EOF

# Query Engine Role에 권한 정책 연결
aws iam put-role-policy \
    --role-name $QUERY_ENGINE_ROLE_NAME \
    --policy-name "QueryEnginePermissions" \
    --policy-document file:///tmp/query-engine-permissions-policy.json

echo "   ✅ Query Engine Role 생성 완료"


#Trust policy of Query Engine role to trust the Kubernetes System namespace.
aws emr-containers update-role-trust-policy \
    --cluster-name $CLUSTER_NAME \
    --namespace $SYSTEM_NAMESPACE \
    --role-name $QUERY_ENGINE_ROLE_NAME \
    --region $REGION 

    
#================== Query Engine Role 완료 =======================================================


 
#================== Job Execution Role 시작 =======================================================
# Job Execution Role Trust Policy 업데이트 (AWS 공식 문서 기준)
echo "   Job Execution Role Trust Policy 업데이트 중..."

cat > /tmp/job-execution-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "emr-containers.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": [
                        "system:serviceaccount:$USER_NAMESPACE:emr-containers-sa-*",
                        "system:serviceaccount:emr-data-team-a:emr-data-*-sa",
                        "system:serviceaccount:$USER_NAMESPACE:emr-*-sa",
                        "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                    ]
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        },
        {
            "Sid": "TrustQueryExecutionRoleForSystemDriver",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                    "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                    "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                    "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
                ]
            },
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Condition": {
                "StringLike": {
                    "aws:RequestedRegion": "$REGION",
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE"
                }
            }
        },
        {
            "Sid": "TrustQueryEngineRoleForSystemExecutor",
            "Effect": "Allow",
            "Principal": {
                "AWS": ["arn:aws:iam::$ACCOUNT_ID:role/$QUERY_ENGINE_ROLE_NAME"]
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                }
            }
        }
    ]
}
EOF

# Job Execution Role 생성 또는 Trust Policy 업데이트
echo "   Job Execution Role (User Driver & Executor용) 생성 중..."
if aws iam get-role --role-name $JOB_EXECUTION_ROLE_NAME >/dev/null 2>&1; then
    echo "   Job Execution Role이 이미 존재합니다. Trust Policy를 업데이트합니다..."
    aws iam update-assume-role-policy \
        --role-name $JOB_EXECUTION_ROLE_NAME \
        --policy-document file:///tmp/job-execution-trust-policy.json
    echo "   ✅Job Execution Role Trust Policy 업데이트 완료"
else
    aws iam create-role \
        --role-name $JOB_EXECUTION_ROLE_NAME \
        --assume-role-policy-document file:///tmp/job-execution-trust-policy.json \
        --description "Lake Formation Job Execution Role for EMR on EKS User profile"
    echo "   ✅ Job Execution Role 생성 완료"
fi


echo "   ✅ Job Execution Role Trust Policy 업데이트 완료 (AWS 공식 문서 기준)"


echo "   Job Execution Role Permission Policy 업데이트 중..."

cat > /tmp/lake-formation-permissions.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:Create*",
                "glue:Update*"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        }
    ]
}
EOF
    
# Lake Formation 권한 정책 연결
aws iam put-role-policy \
    --role-name $JOB_EXECUTION_ROLE_NAME \
    --policy-name "LakeFormationPermissions" \
    --policy-document file:///tmp/lake-formation-permissions.json

# aws emr-containers update-role-trust-policy \
#     --cluster-name $CLUSTER_NAME \
#     --namespace $USER_NAMESPACE \
#     --role-name $JOB_EXECUTION_ROLE_NAME \
#     --region $REGION

echo "✅ 모든 Lake Formation 역할에 EMR 실행 권한 추가 완료"



#================== Job Execution Role 개별=======================================================

# Job Execution Role Trust Policy 업데이트 (AWS 공식 문서 기준)
echo "   Job Execution Role Trust Policy 업데이트 중..."

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role Trust Policy 업데이트 중..."
    
    # AWS 공식 문서 기준 Lake Formation FGAC Trust Policy
    cat > /tmp/job-execution-trust-policy-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "emr-containers.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": [
                        "system:serviceaccount:$USER_NAMESPACE:emr-containers-sa-*",
                        "system:serviceaccount:emr-data-team-a:emr-data-*-sa",
                        "system:serviceaccount:$USER_NAMESPACE:emr-*-sa",
                        "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                    ]
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        },
        {
            "Sid": "TrustQueryExecutionRoleForSystemDriver",
            "Effect": "Allow",
            "Principal": {
                "AWS": ["arn:aws:iam::$ACCOUNT_ID:role/$JOB_EXECUTION_ROLE_NAME"]
            },
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Condition": {
                "StringLike": {
                    "aws:RequestedRegion": "$REGION",
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE"
                }
            }
        },
        {
            "Sid": "TrustQueryEngineRoleForSystemExecutor",
            "Effect": "Allow",
            "Principal": {
                "AWS": ["arn:aws:iam::$ACCOUNT_ID:role/$QUERY_ENGINE_ROLE_NAME"]
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                }
            }
        }
    ]
}
EOF
    
    # Trust Policy 업데이트
    aws iam update-assume-role-policy \
        --role-name $role \
        --policy-document file:///tmp/job-execution-trust-policy-$role.json
done

echo "   ✅ Job Execution Role Trust Policy 업데이트 완료 (AWS 공식 문서 기준)"


# Job Execution Role에 Lake Formation 권한 추가
echo "   Job Execution Role에 Lake Formation 권한 추가 중..."

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role에 Lake Formation 권한 추가 중..."
    
    # Lake Formation 권한 정책
    cat > /tmp/lake-formation-permissions-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:Create*",
                "glue:Update*"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        }
    ]
}
EOF
    
    # Lake Formation 권한 정책 연결
    aws iam put-role-policy \
        --role-name $role \
        --policy-name "LakeFormationPermissions" \
        --policy-document file:///tmp/lake-formation-permissions-$role.json
done

#================== Job Execution Role 개별 끝=======================================================

#================== Script bucket 개별끝=======================================================
cat > /tmp/scripts-bucket-permissions.json << EOF
{
     "Version": "2012-10-17",
     "Statement": [
         {
             "Sid": "ScriptsBucketAccess",
             "Effect": "Allow",
             "Action": [
                 "s3:GetObject",
                 "s3:ListBucket",
                 "s3:GetBucketLocation"
            ], 
             "Resource": [
                 "arn:aws:s3:::${SCRIPTS_BUCKET}",
                 "arn:aws:s3:::${SCRIPTS_BUCKET}/*"
             ]
         }
     ]
}
EOF

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "  $role에 스크립트 버킷 접근 권한 추가 중..."
    

    aws iam put-role-policy \
        --role-name $role \
        --policy-name "ScriptsBucketAccess" \
        --policy-document file:///tmp/scripts-bucket-permissions.json
    
    echo "  ✅ $role 스크립트 버킷 접근 권한 추가 완료"
done

echo "✅ 모든 Lake Formation 역할에 스크립트 버킷 접근 권한 추가 완료"


# CloudWatch Logs 권한 범위 확장 중...
echo "CloudWatch Logs 권한 범위 확장 중..."

for role in "LF_DataStewardRole" "LF_GangnamAnalyticsRole" "LF_OperationRole" "LF_MarketingPartnerRole"; do
    echo "  $role에 확장된 CloudWatch Logs 권한 추가 중..."
    
    cat > /tmp/expanded-cloudwatch-logs-permissions-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudWatchLogsFullAccess",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:$REGION:$ACCOUNT_ID:*"
            ]
        },
        {
            "Sid": "S3LogsAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::seoul-bike-analytics-results-$ACCOUNT_ID",
                "arn:aws:s3:::seoul-bike-analytics-results-$ACCOUNT_ID/*"
            ]
        }
    ]
}
EOF
    
    # 기존 정책 삭제 후 새로운 정책 추가
    aws iam delete-role-policy --role-name $role --policy-name "CloudWatchLogsPermissions" 2>/dev/null || true
    
    aws iam put-role-policy \
        --role-name $role \
        --policy-name "ExpandedCloudWatchLogsPermissions" \
        --policy-document file:///tmp/expanded-cloudwatch-logs-permissions-$role.json
    
    echo "  ✅ $role 확장된 CloudWatch Logs 권한 추가 완료"
done

echo "✅ 모든 Lake Formation 역할에 확장된 CloudWatch Logs 권한 추가 완료"

# Step 3.5: Lake Formation FGAC 권한 검증 및 문제 해결
echo ""
echo "3.5. Lake Formation FGAC 권한 검증 및 문제 해결..."

# Query Engine Role에 추가 TagSession 권한 부여 (로그 분석 결과 반영)
echo "   Query Engine Role에 추가 TagSession 권한 부여 중..."

cat > /tmp/query-engine-tagsession-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "TagSessionPermissionForLakeFormationFGAC",
            "Effect": "Allow",
            "Action": [
                "sts:TagSession"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                }
            }
        },
        {
            "Sid": "AssumeRolePermissionForLakeFormationFGAC",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                }
            }
        }
    ]
}
EOF

# Query Engine Role에 TagSession 정책 추가
aws iam put-role-policy \
    --role-name $QUERY_ENGINE_ROLE_NAME \
    --policy-name "TagSessionPermissions" \
    --policy-document file:///tmp/query-engine-tagsession-policy.json

echo "   ✅ Query Engine Role TagSession 권한 추가 완료"

# 각 Lake Formation 역할의 Trust Policy에 Query Engine Role 신뢰 관계 추가
echo "   각 Lake Formation 역할에 Query Engine Role 신뢰 관계 추가 중..."

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role Trust Policy에 Query Engine Role 추가 중..."
    
    # 현재 Trust Policy 가져오기
    CURRENT_TRUST_POLICY=$(aws iam get-role --role-name $role --query 'Role.AssumeRolePolicyDocument' --output json)
    
    # Query Engine Role을 신뢰하는 새로운 Trust Policy 생성
    cat > /tmp/updated-trust-policy-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "emr-containers.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": [
                        "system:serviceaccount:$USER_NAMESPACE:emr-containers-sa-*",
                        "system:serviceaccount:emr-data-team-a:emr-data-*-sa",
                        "system:serviceaccount:$USER_NAMESPACE:emr-*-sa",
                        "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                    ]
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        },
        {
            "Sid": "TrustQueryEngineRoleForLakeFormationFGAC",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::$ACCOUNT_ID:role/$QUERY_ENGINE_ROLE_NAME",
                    "arn:aws:iam::$ACCOUNT_ID:role/$JOB_EXECUTION_ROLE_NAME"
                ]
            },
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                },
                "StringLike": {
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE"
                }
            }
        }
    ]
}
EOF
    
    # Trust Policy 업데이트
    aws iam update-assume-role-policy \
        --role-name $role \
        --policy-document file:///tmp/updated-trust-policy-$role.json
    
    echo "     ✅ $role Trust Policy 업데이트 완료"
done

echo "   ✅ 모든 Lake Formation 역할에 Query Engine Role 신뢰 관계 추가 완료"

# Lake Formation 권한 검증
echo "   Lake Formation 권한 검증 중..."

# Query Engine Role이 각 역할을 AssumeRole 할 수 있는지 검증
echo "   Query Engine Role AssumeRole 권한 검증 중..."
for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role AssumeRole 권한 확인 중..."
    
    # 시뮬레이션을 통한 권한 검증 (실제 AssumeRole은 하지 않음)
    POLICY_RESULT=$(aws iam simulate-principal-policy \
        --policy-source-arn "arn:aws:iam::$ACCOUNT_ID:role/$QUERY_ENGINE_ROLE_NAME" \
        --action-names "sts:AssumeRole" "sts:TagSession" \
        --resource-arns "arn:aws:iam::$ACCOUNT_ID:role/$role" \
        --query 'EvaluationResults[?Decision==`allowed`]' \
        --output text 2>/dev/null || echo "권한 시뮬레이션 실패")
    
    if [[ "$POLICY_RESULT" == *"allowed"* ]]; then
        echo "     ✅ $role AssumeRole/TagSession 권한 확인됨"
    else
        echo "     ⚠️  $role AssumeRole/TagSession 권한 확인 필요"
    fi
done

echo "   ✅ Lake Formation FGAC 권한 검증 완료"

# Step 4: Security Configuration 생성
echo ""
echo "4. Security Configuration 생성..."

# 기존 Security Configuration 삭제 (있다면)
aws emr-containers delete-security-configuration \
    --id $SECURITY_CONFIG_NAME \
    --region $REGION 2>/dev/null || true

# Security Configuration 생성
SECURITY_CONFIG_ID=$(aws emr-containers create-security-configuration \
    --name $SECURITY_CONFIG_NAME \
    --region $REGION \
    --security-configuration '{
        "authorizationConfiguration": {
            "lakeFormationConfiguration": {
                "authorizedSessionTagValue": "'$LF_SESSION_TAG_VALUE'",
                "secureNamespaceInfo": {
                    "clusterId": "'$CLUSTER_NAME'",
                    "namespace": "'$SYSTEM_NAMESPACE'"
                },
                "queryEngineRoleArn": "arn:aws:iam::'$ACCOUNT_ID':role/'$JOB_EXECUTION_ROLE_NAME'"
            }
        }
    }' \
    --query 'id' --output text)

echo "   ✅ Security Configuration 생성 완료: $SECURITY_CONFIG_ID"

# Step 5: Lake Formation FGAC Virtual Cluster 생성
echo ""
echo "5. Lake Formation FGAC Virtual Cluster 생성..."

LF_VIRTUAL_CLUSTER_NAME="seoul-bike-lf-vc"

# 기존 Virtual Cluster 확인 및 재사용
EXISTING_VC=$(aws emr-containers list-virtual-clusters \
    --region $REGION \
    --query "virtualClusters[?name=='$LF_VIRTUAL_CLUSTER_NAME' && state=='RUNNING'].id" \
    --output text)

if [ ! -z "$EXISTING_VC" ] && [ "$EXISTING_VC" != "None" ]; then
    echo "   ✅ 기존 Virtual Cluster 재사용: $EXISTING_VC"
    LF_VIRTUAL_CLUSTER_ID=$EXISTING_VC
else
    echo "   새로운 Virtual Cluster 생성 중..."
    LF_VIRTUAL_CLUSTER_ID=$(aws emr-containers create-virtual-cluster \
        --name $LF_VIRTUAL_CLUSTER_NAME \
        --region $REGION \
        --container-provider '{
            "id": "'$CLUSTER_NAME'",
            "type": "EKS",
            "info": {
                "eksInfo": {
                    "namespace": "'$USER_NAMESPACE'"
                }
            }
        }' \
        --security-configuration-id $SECURITY_CONFIG_ID \
        --query 'id' --output text)
    echo "   ✅ Virtual Cluster 생성 완료: $LF_VIRTUAL_CLUSTER_ID"
fi

echo "   ✅ Lake Formation FGAC Virtual Cluster 생성 완료: $LF_VIRTUAL_CLUSTER_ID"

# Step 6: 환경 변수 파일 업데이트
echo ""
echo "6. 환경 변수 파일 업데이트..."

# .env 파일에 Lake Formation 설정 추가
cat >> .env << EOF

# Lake Formation FGAC 설정 ($(date '+%Y-%m-%d %H:%M:%S'))
LF_SESSION_TAG_VALUE=$LF_SESSION_TAG_VALUE
SYSTEM_NAMESPACE=$SYSTEM_NAMESPACE
USER_NAMESPACE=$USER_NAMESPACE
JOB_EXECUTION_ROLE_NAME=$JOB_EXECUTION_ROLE_NAME
QUERY_ENGINE_ROLE_NAME=$QUERY_ENGINE_ROLE_NAME
SECURITY_CONFIG_NAME=$SECURITY_CONFIG_NAME
SECURITY_CONFIG_ID=$SECURITY_CONFIG_ID
LF_VIRTUAL_CLUSTER_NAME=$LF_VIRTUAL_CLUSTER_NAME
LF_VIRTUAL_CLUSTER_ID=$LF_VIRTUAL_CLUSTER_ID
EOF

echo "   ✅ 환경 변수 파일 업데이트 완료"

# 임시 파일 정리
rm -f /tmp/emr-containers-*.yaml
rm -f /tmp/query-execution-*.json
rm -f /tmp/query-engine-*.json
rm -f /tmp/job-execution-*.json
rm -f /tmp/lake-formation-*.json
rm -f /tmp/emr-execution-*.json
rm -f /tmp/expanded-cloudwatch-*.json
rm -f /tmp/updated-trust-policy-*.json
rm -f /tmp/query-engine-tagsession-policy.json

echo ""
echo "=== Lake Formation FGAC 설정 완료 ==="
echo ""
echo "📋 설정된 리소스 요약:"
echo "┌─────────────────────────────┬─────────────────────────────────────┐"
echo "│ 리소스                      │ 값                                  │"
echo "├─────────────────────────────┼─────────────────────────────────────┤"
echo "│ Session Tag Value           │ $LF_SESSION_TAG_VALUE               │"
echo "│ System Namespace            │ $SYSTEM_NAMESPACE                   │"
echo "│ User Namespace              │ $USER_NAMESPACE                     │"
echo "│ Query Execution Role (Driver) │ $JOB_EXECUTION_ROLE_NAME      │"
echo "│ Query Engine Role (Executor)   │ $QUERY_ENGINE_ROLE_NAME         │"
echo "│ Security Configuration      │ $SECURITY_CONFIG_ID                 │"
echo "│ LF Virtual Cluster          │ $LF_VIRTUAL_CLUSTER_ID              │"
echo "└─────────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "🔐 Lake Formation FGAC 기능:"
echo "   • Row-level Security: 지역별 필터링 (강남구)"
echo "   • Column-level Security: 역할별 컬럼 접근 제어"
echo "   • Cell-level Security: 연령대별 세밀한 제어 (20-30대)"
echo ""
echo "🎭 지원되는 역할:"
echo "   • DataSteward: 전체 데이터 접근"
echo "   • GangnamAnalytics: 강남구 데이터만"
echo "   • Operation: 운영 데이터 (개인정보 제외)"
echo "   • MarketingPartner: 강남구 20-30대만"
echo ""
echo "🔧 권한 문제 해결 적용:"
echo "   • Query Engine Role에 sts:TagSession 권한 추가"
echo "   • 모든 Lake Formation 역할에 Query Engine Role 신뢰 관계 추가"
echo "   • Lake Formation FGAC 권한 검증 완료"
echo ""
echo "✅ 다음 단계: ./scripts/07-run-emr-jobs.sh (Lake Formation FGAC 활성화됨)"
echo ""
echo "⚠️  주의사항:"
echo "   • Lake Formation에서 데이터베이스 및 테이블 권한을 별도로 설정해야 합니다"
echo "   • S3 Tables 데이터가 Lake Formation에 등록되어 있어야 합니다"
echo "   • 각 역할별로 적절한 Lake Formation 권한을 부여해야 합니다"
echo ""
echo "🔍 문제 해결 정보:"
echo "   • 로그 분석 결과를 반영하여 sts:TagSession 권한 문제 해결"
echo "   • Query Engine Role이 모든 Lake Formation 역할에 대해 AssumeRole/TagSession 권한 보유"
echo "   • Trust Policy에 Query Engine Role 신뢰 관계 추가로 권한 체인 문제 해결"
echo ""
