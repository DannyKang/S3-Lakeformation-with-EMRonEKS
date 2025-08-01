#!/usr/bin/env python3

from pyspark.sql import SparkSession
import sys

def main():
    print("=== Simple Test 시작 ===")
    
    spark = SparkSession.builder.appName("SimpleTest").getOrCreate()
    
    try:
        print("Spark 세션 생성 완료")
        print(f"Spark 버전: {spark.version}")
        
        # 데이터베이스 목록 확인
        print("\n데이터베이스 목록:")
        databases = spark.sql("SHOW DATABASES")
        databases.show()
        
        # bike_db 데이터베이스 사용
        spark.sql("USE bike_db")
        print("\nbike_db 데이터베이스 사용 중")
        
        # 테이블 목록 확인
        print("\n테이블 목록:")
        tables = spark.sql("SHOW TABLES")
        tables.show()
        
        # 테이블 스키마 확인
        print("\nbike_rental_data 테이블 스키마:")
        spark.sql("DESCRIBE bike_rental_data").show()
        
        # 간단한 카운트 쿼리
        print("\n레코드 수 확인:")
        count_result = spark.sql("SELECT COUNT(*) as total_count FROM bike_rental_data")
        count_result.show()
        
        # 샘플 데이터 확인
        print("\n샘플 데이터 (5건):")
        sample_data = spark.sql("SELECT * FROM bike_rental_data LIMIT 5")
        sample_data.show()
        
        print("\n=== Simple Test 완료 ===")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()