# main.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from crawler import fetch_kosis_data
from insert_to_db import insert_data

# --- 설정 영역 ---
KOSIS_API_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=YjI0YmRkNGZiMjZhZWZmYjI5MzQyNjZkNDAwOWE2YTg=&itmId=T+&objL1=ALL&objL2=00+10+11+12+13+20+&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&startPrdDe=202508&endPrdDe=202508&outputFields=OBJ_NM+NM+ITM_NM+UNIT_NM+PRD_DE+&orgId=101&tblId=DT_1J22004" # API URL은 그대로 두거나 env로 옮겨도 됩니다.
DB_TABLE_NAME = "drought_impact_fresh_food_price_index"

def main():

    load_dotenv()

    env = os.getenv("EXECUTION_ENV", "local")
    print(f"--- 실행 환경: {env} ---")

    try:
        db_host = os.getenv("DB_HOST")
        db_port = int(os.getenv("DB_PORT"))
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        if env == "production":
            print("운영 환경으로 판단하여 RDS에 직접 접속합니다.")
            conn_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            engine = create_engine(conn_str)

            # 2. API 데이터 크롤링
            json_data = fetch_kosis_data(KOSIS_API_URL)

            # 3. 데이터가 있으면 DB 엔진과 함께 적재 함수 호출
            if json_data:
                insert_data(json_data, engine, DB_TABLE_NAME)
        else:
            print("로컬 환경으로 판단하여 SSH 터널링을 시작합니다.")
            ssh_host = os.getenv("SSH_HOST")
            ssh_port = int(os.getenv("SSH_PORT"))
            ssh_user = os.getenv("SSH_USER")
            ssh_pkey = os.getenv("SSH_PKEY")

            with SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_pkey=ssh_pkey,
                    remote_bind_address=(db_host, db_port)
            ) as server:
                local_port = server.local_bind_port
                print(f"SSH 터널이 생성되었습니다. (localhost:{local_port} -> {db_host}:{db_port})")

                conn_str = f'mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{local_port}/{db_name}'
                engine = create_engine(conn_str)

                # 2. API 데이터 크롤링
                json_data = fetch_kosis_data(KOSIS_API_URL)

                # 3. 데이터가 있으면 DB 엔진과 함께 적재 함수 호출
                if json_data:
                    insert_data(json_data, engine, DB_TABLE_NAME)

    except Exception as e:
        print(f"!! 전체 프로세스 실행 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()