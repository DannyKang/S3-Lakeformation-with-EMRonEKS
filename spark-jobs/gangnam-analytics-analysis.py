#!/usr/bin/env python3
"""
Gangnam Analytics Analysis - 강남구 데이터 분석
Lake Formation FGAC 데모용 - 강남구 데이터만 접근 (개인정보 birth_year 제외)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Lake Formation FGAC 지원 Spark 세션 생성"""
    return SparkSession.builder \
        .appName("GangnamAnalytics-DistrictAnalysis") \
        .getOrCreate()

def main():
    print("=== Gangnam Analytics Analysis 시작 ===")
    print("역할: 강남구 분석가 - 강남구 데이터만 접근 (개인정보 제외)")
    
    spark = create_spark_session()
    
    try:
        # Lake Formation FGAC를 통해 데이터 읽기
        print("\n1. Lake Formation FGAC를 통해 데이터 로드 중...")
        namespace = "bike_db"
        table_name = "bike_rental_data"
        
        print(f"   네임스페이스: {namespace}")
        print(f"   테이블명: {table_name}")
        print(f"   카탈로그: glue_catalog.{namespace}.{table_name}")
        
        # Glue 카탈로그를 통해 데이터 읽기
        df = spark.read.table(f"glue_catalog.{namespace}.{table_name}")
        
        total_records = df.count()
        print(f"✅ Lake Formation 필터링 후 레코드 수: {total_records:,}건")
        print("🔒 접근 제한: 개인정보 birth_year 제외")
        
        # 데이터 스키마 확인
        print("\n2. 접근 가능한 데이터 스키마:")
        df.printSchema()
        
        # 기본 통계
        print(f"\n3. 기본 통계 정보:")
        print(f"   • 고유 대여 ID: {df.select('rental_id').distinct().count():,}")
        print(f"   • 고유 정거장: {df.select('station_id').distinct().count():,}")
        print(f"   • 고유 구: {df.select('district').distinct().count()}")
        
        # 구별 분포 확인 (Lake Formation 필터링 결과)
        print(f"\n4. 구별 분포 (Lake Formation 필터링 결과):")
        district_stats = df.groupBy("district") \
                           .agg(count("*").alias("count"),
                                avg("usage_min").alias("avg_usage"),
                                avg("distance_meter").alias("avg_distance")) \
                           .orderBy(desc("count"))
        
        district_stats.show(truncate=False)
        
        # 성별 분포
        print(f"\n5. 성별 분포:")
        gender_stats = df.groupBy("gender") \
                         .agg(count("*").alias("count"),
                              avg("usage_min").alias("avg_usage")) \
                         .orderBy(desc("count"))
        
        gender_stats.show()
        
        # 정거장별 이용 현황 (상위 10개)
        print(f"\n6. 정거장별 이용 현황 (상위 10개):")
        station_stats = df.groupBy("station_id", "station_name") \
                          .agg(count("*").alias("rental_count"),
                               avg("usage_min").alias("avg_usage"),
                               avg("distance_meter").alias("avg_distance")) \
                          .orderBy(desc("rental_count")) \
                          .limit(10)
        
        station_stats.show(truncate=False)
        
        # 대여 시간 통계
        print(f"\n7. 대여 시간 통계:")
        usage_stats = df.select(
            avg("usage_min").alias("평균_대여시간"),
            min("usage_min").alias("최소_대여시간"),
            max("usage_min").alias("최대_대여시간"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_대여시간")
        )
        
        usage_stats.show()
        
        # 이동 거리 통계
        print(f"\n8. 이동 거리 통계:")
        distance_stats = df.select(
            avg("distance_meter").alias("평균_이동거리"),
            min("distance_meter").alias("최소_이동거리"),
            max("distance_meter").alias("최대_이동거리"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리")
        )
        
        distance_stats.show()
        
        # 시간대별 이용 패턴
        print(f"\n9. 시간대별 이용 패턴:")
        hourly_pattern = df.withColumn("hour", hour("rental_date")) \
                           .groupBy("hour") \
                           .count() \
                           .orderBy("hour")
        
        hourly_pattern.show(24)
        
        # 사용자 유형별 분석
        print(f"\n10. 사용자 유형별 분석:")
        user_type_stats = df.groupBy("user_type") \
                            .agg(count("*").alias("count"),
                                 avg("usage_min").alias("avg_usage"),
                                 avg("distance_meter").alias("avg_distance")) \
                            .orderBy(desc("count"))
        
        user_type_stats.show()
        
        # 데이터 품질 검증
        print(f"\n11. 데이터 품질 검증:")
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        print("컬럼별 NULL 값 개수:")
        null_counts.show()
        
        print(f"\n=== Gangnam Analytics Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건")
        print(f"🔒 권한: Lake Formation FGAC 적용 (개인정보 birth_year 제외)")
        print(f"📊 역할: 지역별 분석 및 서비스 기획")
        print(f"🗂️ 카탈로그: Glue Catalog (Apache Iceberg)")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
