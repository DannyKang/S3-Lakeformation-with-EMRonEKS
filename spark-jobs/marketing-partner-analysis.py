#!/usr/bin/env python3
"""
Marketing Partner Analysis - 마케팅 파트너 분석
Lake Formation FGAC 데모용 - 강남구 20-30대 데이터만 접근 (개인정보 birth_year 제외)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Lake Formation FGAC 지원 Spark 세션 생성"""
    return SparkSession.builder \
        .appName("MarketingPartner-TargetAnalysis") \
        .getOrCreate()

def main():
    print("=== Marketing Partner Analysis 시작 ===")
    print("역할: 마케팅 파트너 - 타겟 고객 분석 (개인정보 제외)")
    
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
        
        # 성별 분포 (타겟 마케팅용)
        print(f"\n5. 성별 분포 (타겟 마케팅용):")
        gender_stats = df.groupBy("gender") \
                         .agg(count("*").alias("count"),
                              avg("usage_min").alias("avg_usage"),
                              avg("distance_meter").alias("avg_distance")) \
                         .orderBy(desc("count"))
        
        gender_stats.show()
        
        # 인기 정거장 분석 (마케팅 포인트)
        print(f"\n6. 인기 정거장 분석 (마케팅 포인트 - 상위 10개):")
        popular_stations = df.groupBy("station_id", "station_name") \
                             .agg(count("*").alias("rental_count"),
                                  avg("usage_min").alias("avg_usage"),
                                  countDistinct("rental_id").alias("unique_rentals")) \
                             .orderBy(desc("rental_count")) \
                             .limit(10)
        
        popular_stations.show(truncate=False)
        
        # 대여 시간 패턴 분석 (마케팅 타이밍)
        print(f"\n7. 대여 시간 패턴 분석 (마케팅 타이밍):")
        usage_patterns = df.select(
            avg("usage_min").alias("평균_대여시간"),
            min("usage_min").alias("최소_대여시간"),
            max("usage_min").alias("최대_대여시간"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_대여시간")
        )
        
        usage_patterns.show()
        
        # 이동 거리 패턴 (서비스 범위 분석)
        print(f"\n8. 이동 거리 패턴 (서비스 범위 분석):")
        distance_patterns = df.select(
            avg("distance_meter").alias("평균_이동거리"),
            min("distance_meter").alias("최소_이동거리"),
            max("distance_meter").alias("최대_이동거리"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리")
        )
        
        distance_patterns.show()
        
        # 시간대별 이용 패턴 (마케팅 시간대 분석)
        print(f"\n9. 시간대별 이용 패턴 (마케팅 시간대 분석):")
        hourly_marketing = df.withColumn("hour", hour("rental_date")) \
                             .groupBy("hour") \
                             .agg(count("*").alias("rental_count"),
                                  avg("usage_min").alias("avg_usage")) \
                             .orderBy("hour")
        
        hourly_marketing.show(24)
        
        # 사용자 유형별 마케팅 분석
        print(f"\n10. 사용자 유형별 마케팅 분석:")
        user_marketing = df.groupBy("user_type") \
                           .agg(count("*").alias("count"),
                                avg("usage_min").alias("avg_usage"),
                                avg("distance_meter").alias("avg_distance"),
                                countDistinct("station_id").alias("station_variety")) \
                           .orderBy(desc("count"))
        
        user_marketing.show()
        
        # 요일별 이용 패턴 (마케팅 캠페인 타이밍)
        print(f"\n11. 요일별 이용 패턴 (마케팅 캠페인 타이밍):")
        weekday_pattern = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                            .groupBy("weekday") \
                            .agg(count("*").alias("rental_count"),
                                 avg("usage_min").alias("avg_usage")) \
                            .orderBy(desc("rental_count"))
        
        weekday_pattern.show()
        
        # 마케팅 인사이트 요약
        print(f"\n12. 마케팅 인사이트 요약:")
        
        # 최고 이용 시간대
        peak_hour = df.withColumn("hour", hour("rental_date")) \
                      .groupBy("hour") \
                      .count() \
                      .orderBy(desc("count")) \
                      .first()
        
        print(f"   • 최고 이용 시간대: {peak_hour['hour']}시 ({peak_hour['count']:,}건)")
        
        # 평균 이용 시간
        avg_usage = df.agg(avg("usage_min")).collect()[0][0]
        print(f"   • 평균 이용 시간: {avg_usage:.1f}분")
        
        # 평균 이동 거리
        avg_distance = df.agg(avg("distance_meter")).collect()[0][0]
        print(f"   • 평균 이동 거리: {avg_distance:.0f}m")
        
        print(f"\n=== Marketing Partner Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건")
        print(f"🔒 권한: Lake Formation FGAC 적용 (개인정보 birth_year 제외)")
        print(f"📊 역할: 타겟 마케팅 분석 및 캠페인 기획")
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
