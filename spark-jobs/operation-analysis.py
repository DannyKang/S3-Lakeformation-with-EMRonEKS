#!/usr/bin/env python3
"""
Operation Analysis - 운영팀 분석
Lake Formation FGAC 데모용 - 운영 관련 데이터만 접근 (개인정보 birth_year, gender 제외)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Lake Formation FGAC 지원 Spark 세션 생성"""
    return SparkSession.builder \
        .appName("Operation-SystemAnalysis") \
        .getOrCreate()

def main():
    print("=== Operation Analysis 시작 ===")
    print("역할: 운영팀 - 시스템 운영 최적화 (개인정보 제외)")
    
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
        print("🔒 접근 제한: 개인정보 birth_year, gender 제외")
        
        # 데이터 스키마 확인
        print("\n2. 접근 가능한 데이터 스키마:")
        df.printSchema()
        
        # 기본 운영 통계
        print(f"\n3. 기본 운영 통계:")
        print(f"   • 총 대여 건수: {total_records:,}")
        print(f"   • 고유 대여 ID: {df.select('rental_id').distinct().count():,}")
        print(f"   • 운영 정거장 수: {df.select('station_id').distinct().count():,}")
        print(f"   • 서비스 구역 수: {df.select('district').distinct().count()}")
        
        # 구별 운영 현황
        print(f"\n4. 구별 운영 현황:")
        district_operations = df.groupBy("district") \
                                .agg(count("*").alias("total_rentals"),
                                     countDistinct("station_id").alias("station_count"),
                                     avg("usage_min").alias("avg_usage"),
                                     avg("distance_meter").alias("avg_distance")) \
                                .orderBy(desc("total_rentals"))
        
        district_operations.show(truncate=False)
        
        # 정거장별 운영 효율성 (상위 15개)
        print(f"\n5. 정거장별 운영 효율성 (상위 15개):")
        station_efficiency = df.groupBy("station_id", "station_name", "district") \
                               .agg(count("*").alias("total_rentals"),
                                    avg("usage_min").alias("avg_usage_min"),
                                    avg("distance_meter").alias("avg_distance_m"),
                                    min("rental_date").alias("first_rental"),
                                    max("rental_date").alias("last_rental")) \
                               .orderBy(desc("total_rentals")) \
                               .limit(15)
        
        station_efficiency.show(truncate=False)
        
        # 시간대별 운영 부하 분석
        print(f"\n6. 시간대별 운영 부하 분석:")
        hourly_load = df.withColumn("hour", hour("rental_date")) \
                        .groupBy("hour") \
                        .agg(count("*").alias("rental_count"),
                             avg("usage_min").alias("avg_usage"),
                             countDistinct("station_id").alias("active_stations")) \
                        .orderBy("hour")
        
        hourly_load.show(24)
        
        # 대여 시간 분포 (운영 최적화용)
        print(f"\n7. 대여 시간 분포 (운영 최적화용):")
        usage_distribution = df.select(
            avg("usage_min").alias("평균_대여시간"),
            min("usage_min").alias("최소_대여시간"),
            max("usage_min").alias("최대_대여시간"),
            expr("percentile_approx(usage_min, 0.25)").alias("25%_대여시간"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_대여시간"),
            expr("percentile_approx(usage_min, 0.75)").alias("75%_대여시간"),
            expr("percentile_approx(usage_min, 0.95)").alias("95%_대여시간")
        )
        
        usage_distribution.show()
        
        # 이동 거리 분포 (서비스 범위 최적화)
        print(f"\n8. 이동 거리 분포 (서비스 범위 최적화):")
        distance_distribution = df.select(
            avg("distance_meter").alias("평균_이동거리"),
            min("distance_meter").alias("최소_이동거리"),
            max("distance_meter").alias("최대_이동거리"),
            expr("percentile_approx(distance_meter, 0.25)").alias("25%_이동거리"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리"),
            expr("percentile_approx(distance_meter, 0.75)").alias("75%_이동거리"),
            expr("percentile_approx(distance_meter, 0.95)").alias("95%_이동거리")
        )
        
        distance_distribution.show()
        
        # 사용자 유형별 운영 패턴
        print(f"\n9. 사용자 유형별 운영 패턴:")
        user_type_operations = df.groupBy("user_type") \
                                 .agg(count("*").alias("rental_count"),
                                      avg("usage_min").alias("avg_usage"),
                                      avg("distance_meter").alias("avg_distance"),
                                      countDistinct("station_id").alias("station_usage")) \
                                 .orderBy(desc("rental_count"))
        
        user_type_operations.show()
        
        # 요일별 운영 패턴
        print(f"\n10. 요일별 운영 패턴:")
        weekday_operations = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                               .withColumn("day_of_week", dayofweek("rental_date")) \
                               .groupBy("day_of_week", "weekday") \
                               .agg(count("*").alias("rental_count"),
                                    avg("usage_min").alias("avg_usage"),
                                    countDistinct("station_id").alias("active_stations")) \
                               .orderBy("day_of_week")
        
        weekday_operations.show()
        
        # 운영 이상치 탐지
        print(f"\n11. 운영 이상치 탐지:")
        operational_outliers = df.filter(
            (col("usage_min") > 480) |  # 8시간 이상 대여
            (col("distance_meter") > 30000) |  # 30km 이상 이동
            (col("usage_min") < 1) |  # 1분 미만 대여
            (col("distance_meter") < 10)  # 10m 미만 이동
        )
        
        outlier_count = operational_outliers.count()
        print(f"운영 이상치 개수: {outlier_count:,}건 ({(outlier_count/total_records)*100:.2f}%)")
        
        if outlier_count > 0:
            print("운영 이상치 샘플 (상위 5건):")
            operational_outliers.select("rental_id", "station_name", "usage_min", "distance_meter", "district") \
                                .orderBy(desc("usage_min")) \
                                .show(5, truncate=False)
        
        # 시스템 성능 지표
        print(f"\n12. 시스템 성능 지표:")
        
        # 평균 회전율 (정거장당 일일 대여 건수)
        daily_turnover = total_records / df.select("station_id").distinct().count()
        print(f"   • 정거장 평균 회전율: {daily_turnover:.1f}건/정거장")
        
        # 평균 이용 효율성
        avg_efficiency = df.agg(avg(col("distance_meter") / col("usage_min"))).collect()[0][0]
        print(f"   • 평균 이용 효율성: {avg_efficiency:.1f}m/분")
        
        # 서비스 커버리지
        total_stations = df.select("station_id").distinct().count()
        active_stations = df.filter(col("usage_min") > 0).select("station_id").distinct().count()
        coverage = (active_stations / total_stations) * 100
        print(f"   • 서비스 커버리지: {coverage:.1f}% ({active_stations}/{total_stations} 정거장)")
        
        print(f"\n=== Operation Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건")
        print(f"🔒 권한: Lake Formation FGAC 적용 (개인정보 birth_year, gender 제외)")
        print(f"📊 역할: 시스템 운영 최적화 및 정거장 관리")
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
