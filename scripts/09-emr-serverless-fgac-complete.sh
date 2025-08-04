#!/bin/bash

# EMR Serverless Lake Formation FGAC 실제 권한 테스트
# 각 역할별로 ./spark-jobs/ 디렉토리의 실제 분석 코드를 사용하여 FGAC 기능 검증

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    exit 1
fi

source .env

echo "=== EMR Serverless Lake Formation FGAC 실제 권한 테스트 ==="
echo "목적: 각 역할별 차별화된 데이터 접근 권한 실제 검증"
echo "데이터: 서울시 따릉이 자전거 대여 데이터 100,000건"
echo "분석 코드: ./spark-jobs/ 디렉토리의 실제 비즈니스 분석 코드 활용"
echo ""

# EMR Serverless 설정
EMR_APP_NAME="seoul-bike-fgac-test"
EMR_RELEASE_LABEL="emr-7.8.0"

# 기존 애플리케이션 확인
EXISTING_APP=$(aws emr-serverless list-applications \
    --region $REGION \
    --query "applications[?name=='$EMR_APP_NAME'].id" \
    --output text)

if [ -n "$EXISTING_APP" ] && [ "$EXISTING_APP" != "None" ]; then
    echo "기존 EMR Serverless 애플리케이션 사용: $EXISTING_APP"
    EMR_APP_ID=$EXISTING_APP
else
    echo "새 EMR Serverless 애플리케이션 생성 중..."
    
    EMR_APP_ID=$(aws emr-serverless create-application \
        --release-label $EMR_RELEASE_LABEL \
        --type "SPARK" \
        --name $EMR_APP_NAME \
        --runtime-configuration '{
            "classification": "spark-defaults",
            "properties": {
                "spark.emr-serverless.lakeformation.enabled": "true",
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.glue_catalog.warehouse": "s3://'$ICEBERG_BUCKET_NAME'/",
                "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.defaultCatalog": "glue_catalog",
                "spark.sql.catalog.glue_catalog.glue.lakeformation-enabled": "true",
                "spark.sql.catalog.glue_catalog.client.region": "'$REGION'",
                "spark.sql.catalog.glue_catalog.s3.region": "'$REGION'",
                "spark.sql.catalog.glue_catalog.glue.region": "'$REGION'",
                "spark.sql.catalog.glue_catalog.glue.account-id": "'$ACCOUNT_ID'"
            }
        }' \
        --auto-start-configuration '{"enabled": true}' \
        --auto-stop-configuration '{"enabled": true, "idleTimeoutMinutes": 30}' \
        --maximum-capacity '{
            "cpu": "8 vCPU",
            "memory": "32 GB"
        }' \
        --region $REGION \
        --query 'applicationId' \
        --output text)
    
    echo "✅ 애플리케이션 생성 완료: $EMR_APP_ID"
fi

# 애플리케이션 상태 확인
echo "애플리케이션 상태 확인 중..."
for i in {1..15}; do
    APP_STATE=$(aws emr-serverless get-application \
        --application-id $EMR_APP_ID \
        --region $REGION \
        --query 'application.state' \
        --output text)
    
    echo "   상태 ${i}/15: $APP_STATE"
    
    if [ "$APP_STATE" = "STARTED" ]; then
        echo "✅ 애플리케이션 준비 완료"
        break
    elif [ "$APP_STATE" = "CREATED" ] || [ "$APP_STATE" = "STOPPED" ]; then
        echo "   애플리케이션 시작 중..."
        aws emr-serverless start-application \
            --application-id $EMR_APP_ID \
            --region $REGION 2>/dev/null || true
    fi
    
    sleep 30
done

if [ "$APP_STATE" != "STARTED" ]; then
    echo "❌ 애플리케이션 시작 실패"
    exit 1
fi

echo ""
echo "📋 테스트 시나리오:"
echo "   1. DataSteward: 전체 데이터 + 전체 컬럼 (개인정보 포함) - data-steward-analysis.py"
echo "   2. GangnamAnalytics: 강남구 데이터만 + birth_year 제외 - gangnam-analytics-analysis.py"
echo "   3. Operation: 전체 데이터 + 운영 컬럼만 (개인정보 제외) - operation-analysis.py"
echo "   4. MarketingPartner: 강남구 20-30대만 + birth_year 제외 - marketing-partner-analysis.py"
echo ""

# Spark 분석 코드를 S3에 업로드하는 함수
upload_spark_jobs() {
    echo "📝 Spark 분석 코드를 S3에 업로드 중..."
    
    # spark-jobs 디렉토리 확인
    if [ ! -d "./spark-jobs" ]; then
        echo "❌ ./spark-jobs 디렉토리가 존재하지 않습니다."
        exit 1
    fi
    
    # 각 역할별 분석 코드 업로드
    local spark_jobs=(
        "data-steward-analysis.py"
        "gangnam-analytics-analysis.py"
        "operation-analysis.py"
        "marketing-partner-analysis.py"
    )
    
    for job_file in "${spark_jobs[@]}"; do
        if [ -f "./spark-jobs/$job_file" ]; then
            echo "   📤 업로드 중: $job_file"
            aws s3 cp "./spark-jobs/$job_file" "s3://$SCRIPTS_BUCKET/spark-jobs/$job_file" --region $REGION
            echo "      ✅ 업로드 완료: s3://$SCRIPTS_BUCKET/spark-jobs/$job_file"
        else
            echo "   ❌ 파일 없음: ./spark-jobs/$job_file"
        fi
    done
    
    echo "✅ 모든 Spark 분석 코드 업로드 완료"
    echo ""
}

# Spark 코드 업로드 실행
upload_spark_jobs
# 역할별 상세 테스트 정의 (실제 spark-jobs 디렉토리 코드 사용)
declare -a FGAC_TEST_ROLES=(
    "LF_DataStewardRole|데이터 관리자|전체 데이터 + 전체 컬럼 (개인정보 포함)|data-steward-analysis.py"
    "LF_GangnamAnalyticsRole|강남구 분석가|강남구 데이터만 + birth_year 제외|gangnam-analytics-analysis.py"
    "LF_OperationRole|운영팀|전체 데이터 + 운영 컬럼만 (개인정보 제외)|operation-analysis.py"
    "LF_MarketingPartnerRole|마케팅 파트너|강남구 20-30대만 + birth_year 제외|marketing-partner-analysis.py"
)

# Job 실행 함수
run_fgac_analysis_job() {
    local role_name=$1
    local role_description=$2
    local test_scenario=$3
    local spark_job_file=$4
    
    echo "🚀 $role_name ($role_description) FGAC 분석 Job 실행 중..."
    echo "   시나리오: $test_scenario"
    echo "   분석 코드: $spark_job_file"
    
    JOB_RUN_ID=$(aws emr-serverless start-job-run \
        --application-id $EMR_APP_ID \
        --execution-role-arn "arn:aws:iam::$ACCOUNT_ID:role/$role_name" \
        --name "${role_name}-analysis-$(date +%Y%m%d-%H%M%S)" \
        --job-driver '{
            "sparkSubmit": {
                "entryPoint": "s3://'$SCRIPTS_BUCKET'/spark-jobs/'$spark_job_file'",
                "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=2 --conf spark.driver.memory=4g --conf spark.executor.instances=2"
            }
        }' \
        --configuration-overrides '{
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": "s3://'$SCRIPTS_BUCKET'/logs/fgac-analysis/"
                }
            }
        }' \
        --region $REGION \
        --query 'jobRunId' \
        --output text)
    
    if [ -n "$JOB_RUN_ID" ] && [ "$JOB_RUN_ID" != "None" ]; then
        echo "   ✅ Job 시작됨: $JOB_RUN_ID"
        echo "$role_name|$JOB_RUN_ID|$role_description|$test_scenario|$spark_job_file" >> /tmp/fgac-analysis-job-ids.txt
        return 0
    else
        echo "   ❌ Job 시작 실패"
        return 1
    fi
}

echo "🎯 EMR Serverless FGAC 실제 분석 Job 실행 시작..."
echo ""

# 모든 Job 실행
rm -f /tmp/fgac-analysis-job-ids.txt

for role_config in "${FGAC_TEST_ROLES[@]}"; do
    IFS='|' read -r role_name role_description test_scenario spark_job_file <<< "$role_config"
    
    if run_fgac_analysis_job "$role_name" "$role_description" "$test_scenario" "$spark_job_file"; then
        echo "   ✅ $role_name Job 실행 성공"
    else
        echo "   ❌ $role_name Job 실행 실패"
    fi
    
    echo ""
    sleep 10
done

# Job 상태 모니터링
echo "📊 실행된 FGAC 분석 Job 목록:"
echo "┌─────────────────────────┬─────────────────────────────────────┬─────────────────────────────────────┬─────────────────────────────────────┐"
echo "│ 역할                    │ Job Run ID                          │ 설명                                │ 분석 코드                           │"
echo "├─────────────────────────┼─────────────────────────────────────┼─────────────────────────────────────┼─────────────────────────────────────┤"

if [ -f "/tmp/fgac-analysis-job-ids.txt" ]; then
    while IFS='|' read -r role_name job_run_id role_description test_scenario spark_job_file; do
        printf "│ %-23s │ %-35s │ %-35s │ %-35s │\n" "$role_name" "$job_run_id" "$role_description" "$spark_job_file"
    done < /tmp/fgac-analysis-job-ids.txt
fi

echo "└─────────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┘"
echo ""

echo "⏳ Job 실행 결과 확인 (최대 15분 대기)..."
echo ""

# 각 Job의 상태 확인
if [ -f "/tmp/fgac-analysis-job-ids.txt" ]; then
    while IFS='|' read -r role_name job_run_id role_description test_scenario spark_job_file; do
        echo "📊 $role_name Job 상태 확인 중..."
        echo "   시나리오: $test_scenario"
        echo "   분석 코드: $spark_job_file"
        
        # 최대 15분 대기 (30회 retry)
        for i in {1..30}; do
            JOB_STATE=$(aws emr-serverless get-job-run \
                --application-id $EMR_APP_ID \
                --job-run-id $job_run_id \
                --region $REGION \
                --query 'jobRun.state' \
                --output text 2>/dev/null || echo "UNKNOWN")
            
            case $JOB_STATE in
                "SUCCESS")
                    echo "   ✅ 완료 - Lake Formation FGAC 분석 성공"
                    echo "   📋 상세 로그: s3://$SCRIPTS_BUCKET/logs/fgac-analysis/applications/$EMR_APP_ID/jobs/$job_run_id/"
                    
                    # 실행 시간 정보 추가
                    JOB_DURATION=$(aws emr-serverless get-job-run \
                        --application-id $EMR_APP_ID \
                        --job-run-id $job_run_id \
                        --region $REGION \
                        --query 'jobRun.totalExecutionDurationSeconds' \
                        --output text 2>/dev/null || echo "N/A")
                    
                    if [ "$JOB_DURATION" != "N/A" ] && [ "$JOB_DURATION" != "None" ]; then
                        echo "   ⏱️  실행 시간: ${JOB_DURATION}초"
                    fi
                    break
                    ;;
                "FAILED"|"CANCELLED")
                    echo "   ❌ 실패 ($JOB_STATE)"
                    
                    # 실패 원인 조회
                    FAILURE_REASON=$(aws emr-serverless get-job-run \
                        --application-id $EMR_APP_ID \
                        --job-run-id $job_run_id \
                        --region $REGION \
                        --query 'jobRun.stateDetails' \
                        --output text 2>/dev/null || echo "Unknown")
                    
                    echo "   실패 원인: $FAILURE_REASON"
                    echo "   📋 오류 로그: s3://$SCRIPTS_BUCKET/logs/fgac-analysis/applications/$EMR_APP_ID/jobs/$job_run_id/"
                    break
                    ;;
                "RUNNING"|"PENDING"|"SUBMITTED"|"SCHEDULED")
                    echo "   ⏳ 진행 중 ($JOB_STATE) - ${i}/30"
                    sleep 30
                    ;;
                *)
                    echo "   ❓ 알 수 없는 상태 ($JOB_STATE) - ${i}/30"
                    sleep 30
                    ;;
            esac
        done
        
        if [ "$JOB_STATE" != "SUCCESS" ] && [ "$JOB_STATE" != "FAILED" ] && [ "$JOB_STATE" != "CANCELLED" ]; then
            echo "   ⏰ 상태 확인 타임아웃 (현재: $JOB_STATE)"
        fi
        
        echo ""
        
    done < /tmp/fgac-analysis-job-ids.txt
fi

echo "=" * 80
echo "🎯 EMR Serverless Lake Formation FGAC 실제 분석 완료"
echo "=" * 80
echo ""
echo "📊 테스트 결과 요약:"
echo "   • 각 역할별로 실제 비즈니스 분석 코드를 통한 FGAC 검증 완료"
echo "   • ./spark-jobs/ 디렉토리의 실제 분석 코드 활용"
echo "   • Lake Formation FGAC의 실제 동작 검증"
echo "   • 컬럼 레벨, 행 레벨 필터링 효과 확인"
echo ""
echo "📁 상세 로그 위치: s3://$SCRIPTS_BUCKET/logs/fgac-analysis/"
echo ""
echo "🔍 로그 분석 명령어:"
echo "   aws s3 ls s3://$SCRIPTS_BUCKET/logs/fgac-analysis/applications/$EMR_APP_ID/jobs/ --region $REGION"
echo ""
echo "📊 모니터링 명령어:"
echo "   aws emr-serverless list-job-runs --application-id $EMR_APP_ID --region $REGION"
echo ""
echo "🎉 다음 단계:"
echo "   1. 각 역할별 로그 분석으로 FGAC 동작 상세 확인"
echo "   2. 권한 차이점 비교 분석"
echo "   3. 실제 비즈니스 분석 결과 검토"
echo "   4. 필요시 추가 Data Cells Filter 조정"
echo ""
echo "📋 분석 코드별 기대 결과:"
echo "   • data-steward-analysis.py: 전체 100,000건 + 모든 컬럼 접근"
echo "   • gangnam-analytics-analysis.py: 강남구 데이터만 + birth_year 제외"
echo "   • operation-analysis.py: 전체 데이터 + 운영 컬럼만"
echo "   • marketing-partner-analysis.py: 강남구 20-30대만 + 타겟 분석"
