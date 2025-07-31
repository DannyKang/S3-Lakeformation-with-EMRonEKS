#!/usr/bin/env python3

import boto3
import json

def check_glue_table():
    glue = boto3.client('glue', region_name='ap-northeast-2')
    
    try:
        # 데이터베이스 확인
        print("=== 데이터베이스 확인 ===")
        databases = glue.get_databases()
        for db in databases['DatabaseList']:
            print(f"데이터베이스: {db['Name']}")
        
        # bike_db 테이블 목록 확인
        print("\n=== bike_db 테이블 목록 ===")
        try:
            tables = glue.get_tables(DatabaseName='bike_db')
            for table in tables['TableList']:
                print(f"테이블: {table['Name']}")
                print(f"  StorageDescriptor: {table.get('StorageDescriptor', {})}")
                if 'StorageDescriptor' in table:
                    sd = table['StorageDescriptor']
                    print(f"  InputFormat: {sd.get('InputFormat', 'NULL')}")
                    print(f"  OutputFormat: {sd.get('OutputFormat', 'NULL')}")
                    print(f"  SerdeInfo: {sd.get('SerdeInfo', {})}")
                    print(f"  Location: {sd.get('Location', 'NULL')}")
        except Exception as e:
            print(f"bike_db 테이블 조회 오류: {e}")
        
        # bike_rental_data 테이블 상세 정보
        print("\n=== bike_rental_data 테이블 상세 정보 ===")
        try:
            table = glue.get_table(DatabaseName='bike_db', Name='bike_rental_data')
            print(json.dumps(table['Table'], indent=2, default=str))
        except Exception as e:
            print(f"bike_rental_data 테이블 조회 오류: {e}")
            
    except Exception as e:
        print(f"Glue 조회 오류: {e}")

if __name__ == "__main__":
    check_glue_table()