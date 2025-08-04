# Lake Formation FGAC with S3 Iceberg and EMR Serverless
## 서울시 따릉이 자전거 대여 데이터를 활용한 세밀한 데이터 접근 제어 구현 (EMR Serverless 버전)

[![AWS](https://img.shields.io/badge/AWS-Lake%20Formation-orange)](https://aws.amazon.com/lake-formation/)
[![S3 Iceberg](https://img.shields.io/badge/S3%20Iceberg-Apache%20Iceberg-blue)](https://iceberg.apache.org/)
[![EMR Serverless](https://img.shields.io/badge/EMR%20Serverless-Spark-green)](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)

이 프로젝트는 AWS Lake Formation의 Fine-Grained Access Control(FGAC)을 S3 Iceberg와 **EMR Serverless** 환경에서 구현하는 완전한 데모입니다. **실제 서울시 따릉이 자전거 대여 데이터 100,000건**을 활용하여 4가지 역할별로 차별화된 데이터 접근 제어를 보여줍니다.

## 🏗️ 아키텍처 개요 (EMR Serverless)

```mermaid
graph TB
    subgraph "Data Source"
        CSV["seoul-bike-sample-100k.csv<br/>100,000 records"]
    end
    
    subgraph "S3 Iceberg"
        ST["seoul-bike-iceberg-{ACCOUNT_ID}<br/>Apache Iceberg Format<br/>bike_db.bike_rental_data"]
    end
    
    subgraph "Lake Formation"
        LF["FGAC Policies"]
        RF["Row Filters"]
        CF["Column Filters"]
        CF2["Cell Filters"]
    end
    
    subgraph "EMR Serverless"
        APP["Serverless Application<br/>emr-7.8.0"]
        DS["DataSteward Role"]
        GA["GangnamAnalyst Role"]
        OP["Operation Role"]
        MP["Marketing Partner Role"]
    end
    
    CSV --> ST
    ST --> LF
    LF --> APP
    APP --> DS
    APP --> GA
    APP --> OP
    APP --> MP
```

## 📊 실제 데이터 현황

### 데이터 구조
- **S3 버킷**: `seoul-bike-iceberg-{ACCOUNT_ID}-{TIMESTAMP}` (자동 생성)
- **테이블**: `bike_db.bike_rental_data`
- **형식**: Apache Iceberg (Glue Catalog)
- **컬럼**: 11개 (rental_id, station_id, station_name, usage_min, distance_meter, birth_year, gender 등)
- **데이터 범위**: 서울시 전체 구, 2024년 12월 자전거 대여 데이터 (**100,000건**)

### 📈 실제 데이터 분포

#### 구별 분포 (상위 10개)
- **강서구**: 13,238건 (13.2%)
- **영등포구**: 8,882건 (8.9%)
- **송파구**: 8,240건 (8.2%)
- **양천구**: 6,577건 (6.6%)
- **노원구**: 6,342건 (6.3%)
- **마포구**: 4,307건 (4.3%)
- **광진구**: 4,155건 (4.2%)
- **강동구**: 4,069건 (4.1%)
- **구로구**: 4,403건 (4.4%)
- **성동구**: 3,568건 (3.6%)

#### 성별 분포
- **남성(M)**: 63,630건 (63.6%)
- **여성(F)**: 32,435건 (32.4%)
- **결측치**: 3,902건 (3.9%)

#### 대여 시간 통계
- **평균 대여 시간**: 16.9분
- **최소 대여 시간**: 0분
- **최대 대여 시간**: 849분 (14시간 9분)
- **중앙값**: 9.0분

#### 이동 거리 통계
- **평균 이동 거리**: 1,795.2m (약 1.8km)
- **최소 이동 거리**: 0m
- **최대 이동 거리**: 49,750m (약 49.8km)
- **중앙값**: 1,199.0m (약 1.2km)

## 🔐 Lake Formation FGAC 상세 설명

### Multi-dimensional Access Control

이 프로젝트는 **3차원 데이터 접근 제어**를 구현합니다:

#### 1. **Row-level Security (행 수준 보안)**
```sql
-- 강남구 데이터만 접근
WHERE district = '강남구'

-- 강남구 + 20-30대만 접근  
WHERE district = '강남구' AND (birth_year >= 1994 AND birth_year <= 2004)
```

#### 2. **Column-level Security (컬럼 수준 보안)**
```sql
-- 역할별 접근 가능한 컬럼
DataSteward: 전체 11개 컬럼
GangnamAnalyst: 10개 컬럼 (birth_year 제외)
Operation: 9개 컬럼 (birth_year, gender 제외)
MarketingPartner: 10개 컬럼 (birth_year 제외)
```

#### 3. **Cell-level Security (셀 수준 보안)**
- 특정 조건을 만족하는 셀만 접근 가능
- 연령대별 세밀한 필터링 (20-30대)
- 지역별 + 연령대별 복합 조건

### 실제 비즈니스 시나리오

| 역할 | 접근 구역 | 연령대 | 접근 컬럼 | 개인정보 | 목적 | 예상 결과 |
|------|-----------|--------|-----------|----------|------|-----------| 
| **LF_DataStewardRole** | 전체 구 | 전체 | 전체 11개 | ✅ | 데이터 품질 관리 | 100,000건 |
| **LF_GangnamAnalyticsRole** | 강남구만 | 전체 | 10개 (birth_year 제외) | ❌ | 강남구 분석 | ~3,000건 |
| **LF_OperationRole** | 전체 구 | 전체 | 9개 (운영 관련만) | ❌ | 운영 최적화 | 100,000건 |
| **LF_MarketingPartnerRole** | 강남구만 | 20-30대만 | 10개 (마케팅 관련) | ❌ | 타겟 마케팅 | ~2,000건 |

## 🚀 단계별 실행 가이드 (EMR Serverless)

### 사전 준비사항

```bash
# 1. 프로젝트 클론
git clone https://github.com/DannyKang/S3-Tables-LakeFormation-with-EMRServerless
cd S3-Tables-LakeFormation-with-EMRServerless

# 2. AWS CLI 설정 (ap-northeast-2 리전 사용)
aws configure set region ap-northeast-2

# 3. 필요한 Python 패키지 설치
pip install boto3 pandas matplotlib seaborn
```

⚠️ **중요**: 
- 모든 스크립트는 프로젝트 루트 디렉토리에서 실행해야 합니다
- 스크립트는 **순서대로** 실행해야 합니다 (01 → 02 → 03 → 04 → 05 → 06 → 07 → 07-1)
- 각 단계 완료 후 다음 단계로 진행하세요

---

## 📋 단계별 실행 순서 (EMR Serverless)

### 1단계: S3 Iceberg 버킷 및 환경 설정

```bash
# S3 Iceberg 버킷 생성 (고유한 버킷명 자동 생성)
./scripts/01-create-s3-bucket.sh
```

**자동 생성되는 리소스**:
- **S3 버킷**: `seoul-bike-iceberg-{ACCOUNT_ID}-{TIMESTAMP}`
- **Glue 데이터베이스**: `bike_db`
- **테이블 위치**: `s3://버킷명/data/bike_db/bike_rental_data/`
- **환경 설정 파일**: `.env` (다른 스크립트들이 자동으로 사용)

⚠️ **중요**: `.env` 파일을 삭제하지 마세요. 모든 스크립트가 이 파일의 설정을 사용합니다.

### 2단계: 데이터 적재

```bash
# 로컬 데이터를 S3 Iceberg에 적재
./scripts/02-load-data-to-iceberg.sh
```

**데이터 적재 과정**:
1. **로컬 데이터 검증**: `./sample-data/seoul-bike-sample-100k.csv` (100,000건)
2. **Iceberg 테이블 생성**: Athena를 통한 Apache Iceberg 테이블 생성
3. **S3 업로드**: 임시 S3 버킷에 CSV 파일 업로드
4. **데이터 적재**: INSERT INTO로 Iceberg 테이블에 적재
5. **검증**: 적재된 데이터 건수 확인

**예상 소요 시간**: 5-10분

### 3단계: IAM 역할 생성

```bash
# Lake Formation FGAC용 IAM 역할 생성
./scripts/03-create-iam-roles.sh
```

**생성되는 역할들**:
- **LF_DataStewardRole**: 데이터 관리자 - 전체 데이터 접근
- **LF_GangnamAnalyticsRole**: 강남구 분석가 - 강남구 데이터만
- **LF_OperationRole**: 운영팀 - 운영 데이터만  
- **LF_MarketingPartnerRole**: 마케팅 파트너 - 강남구 20-30대만

### 4단계: Lake Formation FGAC 권한 설정

```bash
# Lake Formation FGAC 권한 설정
./scripts/04-setup-lakeformation-permissions-iceberg.sh
```

**설정되는 권한**:
- **Database 권한**: bike_db 데이터베이스 접근 권한
- **Table 권한**: bike_rental_data 테이블 기본 권한
- **Data Cells Filter**: 역할별 행/컬럼 필터 생성
- **Location 권한**: S3 Iceberg 버킷 접근 권한

### 5단계: EMR on EKS 클러스터 설정 (선택사항)

```bash
# EMR on EKS 환경 구성 (비교 분석용)
./scripts/05-setup-emr-on-eks.sh
```

**설정되는 리소스**:
- **EKS 클러스터**: seoul-bike-emr (Karpenter 기반 자동 스케일링)
- **네임스페이스**: emr-data-team
- **Virtual Cluster**: seoul-bike-emr-vc
- **서비스 계정**: 4개 (역할별 IRSA 연결)
- **Spark 코드 버킷**: seoul-bike-analytics-scripts-{ACCOUNT_ID}

**예상 소요 시간**: 15-20분

### 6단계: Lake Formation FGAC 설정

```bash
# Lake Formation FGAC 세부 설정
./scripts/06-setup-lake-formation-fgac.sh
```

**설정되는 FGAC 권한**:
- **EMR on EKS FGAC**: Virtual Cluster 및 Security Configuration
- **역할별 세부 권한**: 행/컬럼/셀 수준 접근 제어
- **Session Tag**: LakeFormationAuthorizedCaller 설정

### 7단계: EMR on EKS Job 실행 (비교용)

```bash
# 역할별 EMR on EKS Job 실행 (비교 분석용)
./scripts/07-run-emr-jobs.sh
```

**실행되는 분석 Job**:
- **data-steward-analysis**: 전체 데이터 품질 관리 및 거버넌스 분석
- **gangnam-analytics-analysis**: 강남구 지역 특화 분석 및 서비스 기획
- **operation-analysis**: 시스템 운영 최적화 및 정거장 관리 분석
- **marketing-partner-analysis**: 강남구 20-30대 타겟 마케팅 분석

### 🆕 7-1단계: EMR Serverless Job 실행 (메인)

```bash
# 역할별 EMR Serverless Job 실행
./scripts/07-1-emr-serverless-job-run.sh
```

**EMR Serverless 특징**:
- **서버리스 아키텍처**: 인프라 관리 불필요
- **자동 스케일링**: 워크로드에 따른 자동 리소스 조정
- **비용 최적화**: 사용한 만큼만 과금
- **Lake Formation 네이티브 지원**: EMR 7.2.0+ 버전에서 완전 지원

**실행되는 과정**:
1. **IAM 역할 Trust Policy 업데이트**: EMR Serverless 서비스 추가
2. **EMR Serverless 애플리케이션 생성**: Lake Formation 활성화
3. **Job 실행**: 동일한 IAM 역할로 권한 비교 분석
4. **결과 모니터링**: EMR on EKS vs EMR Serverless 비교

**Job 모니터링**:
```bash
# EMR Serverless 애플리케이션 목록
aws emr-serverless list-applications --region ap-northeast-2

# 특정 Job 상세 정보
aws emr-serverless get-job-run --application-id {APPLICATION_ID} --job-run-id {JOB_RUN_ID} --region ap-northeast-2
```

### 8단계: 결과 검증 및 분석

```bash
# 권한 검증 및 결과 분석
./scripts/08-verify-and-analyze.sh
```

**검증 내용**:
- **FGAC 권한 검증**: 각 역할별 접근 가능한 데이터 확인
- **EMR on EKS vs EMR Serverless 비교**: 권한 처리 방식 차이 분석
- **결과 분석**: Job 실행 결과 및 성능 분석
- **리포트 생성**: 역할별 분석 결과 요약

## 📁 프로젝트 구조 (EMR Serverless)

```
lake-formation-fgac-demo/
├── README.md                                    # EMR on EKS 버전 가이드
├── README-serverless.md                         # EMR Serverless 버전 가이드 (이 파일)
├── data-dictionary.md                           # 데이터 사전
├── scripts/                                     # 실행 스크립트 (8단계)
│   ├── 01-create-s3-bucket.sh                  # S3 Iceberg 버킷 생성
│   ├── 02-load-data-to-iceberg.sh              # 데이터 적재
│   ├── 03-create-iam-roles.sh                  # IAM 역할 생성
│   ├── 04-setup-lakeformation-permissions-iceberg.sh # Lake Formation FGAC 권한
│   ├── 05-setup-emr-on-eks.sh                  # EMR on EKS 설정 (선택사항)
│   ├── 06-setup-lake-formation-fgac.sh         # Lake Formation FGAC 세부 설정
│   ├── 07-run-emr-jobs.sh                      # EMR on EKS Job 실행 (비교용)
│   ├── 07-1-emr-serverless-job-run.sh          # 🆕 EMR Serverless Job 실행 (메인)
│   └── 08-verify-and-analyze.sh                # 검증 및 분석
├── spark-jobs/                                 # Spark 분석 코드 (4개 역할)
│   ├── data-steward-analysis.py                # 데이터 관리자 분석
│   ├── gangnam-analytics-analysis.py           # 강남구 분석가 분석
│   ├── operation-analysis.py                   # 운영팀 분석
│   └── marketing-partner-analysis.py           # 마케팅 파트너 분석
├── sample-data/                                # 샘플 데이터
│   ├── seoul-bike-sample-100k.csv              # 100,000건 샘플 데이터
│   ├── seoul-bike-sample-preview.csv           # 미리보기용 50건
│   └── data-dictionary.md                      # 데이터 사전
├── docs/                                       # 문서
└── results/                                    # 분석 결과 (실행 후 생성)
```

## 🔧 다중 사용자 환경 지원

이 데모는 **여러 사용자가 동시에 따라할 수 있도록** 설계되었습니다:

### 🎯 고유 리소스 생성
- **S3 버킷**: `seoul-bike-iceberg-{ACCOUNT_ID}-{TIMESTAMP}` 형식으로 자동 생성
- **IAM 역할**: 계정별로 독립적으로 생성
- **EMR Serverless 애플리케이션**: 사용자별 고유한 애플리케이션명 사용

### 📁 환경 설정 관리
- **`.env` 파일**: 첫 번째 스크립트 실행 시 자동 생성
- **자동 설정 공유**: 모든 스크립트가 `.env` 파일의 설정을 자동으로 사용
- **충돌 방지**: 다른 사용자와 리소스명 충돌 없음

## 🎯 예상 결과 (EMR Serverless)

### Data Steward Role
- 총 대여 건수: **100,000건**
- 접근 가능 구역: **전체 구**
- 평균 대여 시간: **16.9분**
- 평균 이동 거리: **1,795.2m**

### Gangnam Analytics Role
- 총 대여 건수: **~3,000건** (강남구만)
- 접근 가능 구역: **1개 구** (강남구)
- 개인정보 접근: **차단** (birth_year 제외)
- 분석 범위: **강남구 지역 특화 분석**

### Operation Role
- 총 대여 건수: **100,000건**
- 접근 가능 구역: **전체 구**
- 개인정보 접근: **차단** (birth_year, gender 제외)
- 분석 범위: **운영 효율성 및 정거장 이용률**

### Marketing Partner Role
- 총 대여 건수: **~2,000건** (강남구 20-30대만)
- 접근 가능 구역: **1개 구** (강남구)
- 접근 가능 연령대: **20-30대만**
- 분석 범위: **타겟 마케팅 분석**

## 🔑 핵심 학습 포인트 (EMR Serverless)

### 1. EMR Serverless vs EMR on EKS 비교
- **인프라 관리**: EMR Serverless는 완전 관리형, EMR on EKS는 Kubernetes 기반
- **스케일링**: EMR Serverless는 자동, EMR on EKS는 Karpenter 기반
- **비용**: EMR Serverless는 사용량 기반, EMR on EKS는 클러스터 기반
- **Lake Formation 지원**: 두 환경 모두 동일한 FGAC 기능 제공

### 2. Multi-dimensional FGAC
- **Row-level**: 지역별 필터링 (강남구)
- **Column-level**: 역할별 컬럼 접근 제어
- **Cell-level**: 연령대별 세밀한 필터링 (20-30대)

### 3. 실제 비즈니스 시나리오
- 데이터 관리자의 전체 데이터 거버넌스
- 지역별 분석가의 제한된 분석
- 운영팀의 운영 데이터 접근
- 마케팅 파트너의 타겟 고객 분석

### 4. 서버리스 아키텍처 장점
- **운영 부담 감소**: 인프라 관리 불필요
- **비용 효율성**: 사용한 만큼만 과금
- **자동 스케일링**: 워크로드에 따른 자동 조정
- **빠른 시작**: 클러스터 프로비저닝 시간 단축

## 🛠️ 기술 스택 (EMR Serverless)

- **AWS Lake Formation**: Fine-Grained Access Control
- **Amazon S3**: Apache Iceberg 기반 데이터 레이크
- **AWS Glue Catalog**: 메타데이터 관리
- **Amazon EMR Serverless**: 서버리스 Spark 분석 (메인)
- **Amazon EMR on EKS**: Kubernetes 기반 Spark 분석 (비교용)
- **Apache Spark**: 대규모 데이터 처리
- **Apache Iceberg**: 테이블 형식 및 스키마 진화
- **Python**: 데이터 분석 및 시각화

## 🔍 문제 해결 (EMR Serverless)

### 일반적인 문제들

#### 1. EMR Serverless 애플리케이션 생성 실패
```bash
# 애플리케이션 상태 확인
aws emr-serverless get-application --application-id {APPLICATION_ID}

# 애플리케이션 재시작
aws emr-serverless start-application --application-id {APPLICATION_ID}
```

#### 2. Lake Formation 권한 문제
```bash
# Lake Formation 권한 확인
aws lakeformation list-permissions --region ap-northeast-2

# Hybrid Access Mode 확인
aws lakeformation describe-resource --resource-arn "arn:aws:s3:::버킷명"
```

#### 3. IAM 역할 Trust Policy 문제
```bash
# 역할 Trust Policy 확인
aws iam get-role --role-name LF_GangnamAnalyticsRole

# EMR Serverless 서비스 포함 여부 확인
```

#### 4. Job 실행 상태 확인
```bash
# Job 목록 확인
aws emr-serverless list-job-runs --application-id {APPLICATION_ID}

# Job 상세 정보
aws emr-serverless get-job-run --application-id {APPLICATION_ID} --job-run-id {JOB_RUN_ID}
```

## 📚 추가 리소스 (EMR Serverless)

- [AWS EMR Serverless 사용자 가이드](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
- [EMR Serverless Lake Formation 통합](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless-lf-enable.html)
- [AWS Lake Formation 개발자 가이드](https://docs.aws.amazon.com/lake-formation/)
- [AWS Glue Catalog 문서](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Apache Iceberg 문서](https://iceberg.apache.org/)

## 🤝 기여

버그 리포트나 기능 제안은 GitHub Issues를 통해 제출해 주세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 제공됩니다.

---

**⚠️ 주의사항**: 이 데모는 교육 목적으로 제작되었습니다. 프로덕션 환경에서 사용하기 전에 보안 검토를 수행하세요.

**📊 데이터 출처**: 이 프로젝트에서 사용된 데이터는 실제 서울시 따릉이 자전거 대여 데이터를 기반으로 하되, 개인정보 보호를 위해 익명화 및 가공 처리되었습니다.

**🔄 EMR on EKS vs EMR Serverless**: 
- EMR on EKS 버전은 `README.md` 참조
- EMR Serverless 버전은 이 파일(`README-serverless.md`) 참조
- 두 환경 모두 동일한 Lake Formation FGAC 기능을 제공하며, 권한 문제 해결을 위한 비교 분석이 가능합니다.
