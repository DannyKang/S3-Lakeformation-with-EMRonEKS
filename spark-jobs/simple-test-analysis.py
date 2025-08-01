#!/usr/bin/env python3
"""
Simple Test Analysis - Lake Formation 없이 기본 Glue Catalog 테스트
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def create_spark_session():
    """기본 Spark 세션 생성 (Lake Formation FGAC 비활성화)"""
    return SparkSession.builder \
        .appName("Simple-Glue-Catalog-Test") \
        .getOrCreate()

def main():
    print("=== Simple Glue Catalog Test 시작 ===")
    print("Lake Formation FGAC 비활성화 상태에서 테스트")
    
    spark = create_spark_session()
    
    try:
        # 기본 Glue Catalog를 통해 데이터 읽기
        print("\n1. Glue Catalog를 통해 데이터 로드 중...")
        namespace = "bike_db"
        table_name = "bike_rental_data"
        
        print(f"   네임스페이스: {namespace}")
        print(f"   테이블명: {table_name}")
        print(f"   카탈로그: glue_catalog.{namespace}.{table_name}")
        
        # Glue 카탈로그를 명시적으로 지정하여 테이블 데이터 읽기
        df = spark.table(f"glue_catalog.{namespace}.{table_name}")
        
        total_records = df.count()
        print(f"✅ 총 레코드 수: {total_records:,}건")
        
        # 데이터 스키마 확인
        print("\n2. 데이터 스키마:")
        df.printSchema()
        
        # 기본 통계
        print(f"\n3. 기본 통계 정보:")
        print(f"   • 고유 대여 ID: {df.select('rental_id').distinct().count():,}")
        print(f"   • 고유 정거장: {df.select('station_id').distinct().count():,}")
        print(f"   • 고유 구: {df.select('district').distinct().count()}")
        
        # 구별 분포 (상위 5개)
        print(f"\n4. 구별 분포 (상위 5개):")
        district_stats = df.groupBy("district") \
                           .agg(count("*").alias("count")) \
                           .orderBy(desc("count")) \
                           .limit(5)
        
        district_stats.show(truncate=False)
        
        # 성별 분포
        print(f"\n5. 성별 분포:")
        gender_stats = df.groupBy("gender") \
                         .agg(count("*").alias("count")) \
                         .orderBy(desc("count"))
        
        gender_stats.show()
        
        print(f"\n=== Simple Glue Catalog Test 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건")
        print(f"🔑 권한: 기본 Glue Catalog 접근")
        print(f"📊 상태: Lake Formation FGAC 비활성화")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
