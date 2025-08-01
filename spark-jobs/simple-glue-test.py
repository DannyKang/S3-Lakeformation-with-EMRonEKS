#!/usr/bin/env python3
"""
Simple Glue Catalog Test - Lake Formation FGAC
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def main():
    print("=== Simple Glue Catalog Test 시작 ===")
    
    spark = SparkSession.builder \
        .appName("SimpleGlueCatalogTest") \
        .getOrCreate()
    
    try:
        # 카탈로그 정보 확인
        print("\n1. Spark 설정 확인:")
        print(f"   Default Catalog: {spark.conf.get('spark.sql.defaultCatalog', 'spark_catalog')}")
        print(f"   Glue Catalog: {spark.conf.get('spark.sql.catalog.glue_catalog', 'Not Set')}")
        print(f"   Lake Formation: {spark.conf.get('spark.sql.catalog.glue_catalog.lake-formation.enabled', 'Not Set')}")
        
        # 데이터베이스 목록 확인
        print("\n2. 데이터베이스 목록:")
        databases = spark.sql("SHOW DATABASES")
        databases.show()
        
        # 테이블 목록 확인
        print("\n3. bike_db 데이터베이스의 테이블 목록:")
        try:
            tables = spark.sql("SHOW TABLES IN bike_db")
            tables.show()
        except Exception as e:
            print(f"   bike_db 테이블 조회 실패: {e}")
        
        # 직접 테이블 접근 시도
        print("\n4. 테이블 직접 접근 시도:")
        try:
            # 방법 1: 기본 카탈로그 사용
            df1 = spark.table("bike_db.bike_rental_data")
            count1 = df1.count()
            print(f"   방법 1 (기본): {count1:,}건")
        except Exception as e:
            print(f"   방법 1 실패: {e}")
        
        try:
            # 방법 2: Glue 카탈로그 명시
            df2 = spark.table("glue_catalog.bike_db.bike_rental_data")
            count2 = df2.count()
            print(f"   방법 2 (glue_catalog): {count2:,}건")
        except Exception as e:
            print(f"   방법 2 실패: {e}")
        
        # Glue 카탈로그 직접 사용
        print("\n5. Glue 카탈로그 직접 사용:")
        try:
            spark.sql("USE glue_catalog")
            spark.sql("USE bike_db")
            df3 = spark.sql("SELECT COUNT(*) as total FROM bike_rental_data")
            df3.show()
        except Exception as e:
            print(f"   Glue 카탈로그 직접 사용 실패: {e}")
        
        print("\n=== Simple Glue Catalog Test 완료 ===")
        
    except Exception as e:
        print(f"❌ 전체 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()