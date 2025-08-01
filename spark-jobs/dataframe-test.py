#!/usr/bin/env python3
"""
DataFrame API Test - Lake Formation FGAC 호환
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def main():
    print("=== DataFrame API Test 시작 ===")
    
    spark = SparkSession.builder \
        .appName("DataFrameAPITest") \
        .getOrCreate()
    
    try:
        # 1. 카탈로그 설정 확인
        print("\n1. 카탈로그 설정:")
        print(f"   Default Catalog: {spark.conf.get('spark.sql.defaultCatalog', 'spark_catalog')}")
        
        # 2. DataFrame API로 테이블 읽기 시도
        print("\n2. DataFrame API로 테이블 읽기:")
        try:
            # 방법 1: spark.read.table 사용
            df = spark.read.table("bike_db.bike_rental_data")
            print(f"   ✅ DataFrame 생성 성공")
            print(f"   스키마: {len(df.columns)}개 컬럼")
            
            # 간단한 count 시도
            count = df.count()
            print(f"   ✅ 총 레코드 수: {count:,}건")
            
            # 컬럼 목록 출력
            print(f"   컬럼: {df.columns}")
            
            # 샘플 데이터 (첫 5행)
            print("\n3. 샘플 데이터:")
            df.show(5)
            
        except Exception as e:
            print(f"   ❌ DataFrame API 실패: {e}")
        
        print("\n=== DataFrame API Test 완료 ===")
        
    except Exception as e:
        print(f"❌ 전체 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()