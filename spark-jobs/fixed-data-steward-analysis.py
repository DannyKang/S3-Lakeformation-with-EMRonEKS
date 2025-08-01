#!/usr/bin/env python3
"""
Fixed Data Steward Analysis - Lake Formation FGAC 호환
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def main():
    print("=== Fixed Data Steward Analysis 시작 ===")
    
    spark = SparkSession.builder \
        .appName("FixedDataStewardAnalysis") \
        .getOrCreate()
    
    try:
        # 1. 카탈로그 설정 확인
        print("\n1. 카탈로그 설정:")
        print(f"   Default Catalog: {spark.conf.get('spark.sql.defaultCatalog', 'spark_catalog')}")
        print(f"   Lake Formation: {spark.conf.get('spark.sql.catalog.glue_catalog.lake-formation.enabled', 'false')}")
        
        # 2. 데이터베이스 확인
        print("\n2. 데이터베이스 목록:")
        databases = spark.sql("SHOW DATABASES")
        databases.show()
        
        # 3. 테이블 확인
        print("\n3. bike_db의 테이블 목록:")
        tables = spark.sql("SHOW TABLES IN bike_db")
        tables.show()
        
        # 4. 테이블 스키마 확인 (Lake Formation FGAC 호환 방식)
        print("\n4. 테이블 스키마 확인:")
        try:
            # DESCRIBE TABLE 사용 (더 안전한 방식)
            schema_info = spark.sql("DESCRIBE TABLE bike_db.bike_rental_data")
            schema_info.show(truncate=False)
        except Exception as e:
            print(f"   스키마 조회 실패: {e}")
        
        # 5. 데이터 샘플 조회 (LIMIT 사용)
        print("\n5. 데이터 샘플 (상위 5건):")
        try:
            sample_data = spark.sql("SELECT * FROM bike_db.bike_rental_data LIMIT 5")
            sample_data.show(truncate=False)
        except Exception as e:
            print(f"   샘플 데이터 조회 실패: {e}")
        
        # 6. 기본 통계 (COUNT만 먼저 시도)
        print("\n6. 기본 통계:")
        try:
            count_result = spark.sql("SELECT COUNT(*) as total_records FROM bike_db.bike_rental_data")
            count_result.show()
        except Exception as e:
            print(f"   COUNT 조회 실패: {e}")
        
        # 7. 구별 분포 (간단한 GROUP BY)
        print("\n7. 구별 분포 (상위 5개):")
        try:
            district_stats = spark.sql("""
                SELECT district, COUNT(*) as count 
                FROM bike_db.bike_rental_data 
                GROUP BY district 
                ORDER BY count DESC 
                LIMIT 5
            """)
            district_stats.show()
        except Exception as e:
            print(f"   구별 분포 조회 실패: {e}")
        
        # 8. 성별 분포 (개인정보 접근 테스트)
        print("\n8. 성별 분포:")
        try:
            gender_stats = spark.sql("""
                SELECT gender, COUNT(*) as count 
                FROM bike_db.bike_rental_data 
                GROUP BY gender 
                ORDER BY count DESC
            """)
            gender_stats.show()
        except Exception as e:
            print(f"   성별 분포 조회 실패: {e}")
        
        # 9. 연령대별 분포 (birth_year 접근 테스트)
        print("\n9. 연령대별 분포:")
        try:
            age_stats = spark.sql("""
                SELECT 
                    CASE 
                        WHEN birth_year >= 2005 THEN '10대'
                        WHEN birth_year >= 1995 THEN '20대'
                        WHEN birth_year >= 1985 THEN '30대'
                        WHEN birth_year >= 1975 THEN '40대'
                        WHEN birth_year >= 1965 THEN '50대'
                        ELSE '60대+'
                    END as age_group,
                    COUNT(*) as count
                FROM bike_db.bike_rental_data 
                GROUP BY 
                    CASE 
                        WHEN birth_year >= 2005 THEN '10대'
                        WHEN birth_year >= 1995 THEN '20대'
                        WHEN birth_year >= 1985 THEN '30대'
                        WHEN birth_year >= 1975 THEN '40대'
                        WHEN birth_year >= 1965 THEN '50대'
                        ELSE '60대+'
                    END
                ORDER BY count DESC
            """)
            age_stats.show()
        except Exception as e:
            print(f"   연령대별 분포 조회 실패: {e}")
        
        # 10. 사용 시간 통계
        print("\n10. 사용 시간 기본 통계:")
        try:
            usage_stats = spark.sql("""
                SELECT 
                    AVG(usage_min) as avg_usage,
                    MIN(usage_min) as min_usage,
                    MAX(usage_min) as max_usage
                FROM bike_db.bike_rental_data
            """)
            usage_stats.show()
        except Exception as e:
            print(f"   사용 시간 통계 조회 실패: {e}")
        
        print("\n=== Fixed Data Steward Analysis 완료 ===")
        print("✅ Lake Formation FGAC 호환 방식으로 분석 완료")
        
    except Exception as e:
        print(f"❌ 전체 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()