# insert_to_db.py

import pandas as pd
from sqlalchemy.engine import Engine


def insert_data(json_data: list, engine: Engine, table_name: str):
    """
    JSON 데이터를 변환하고, 전달받은 DB 엔진을 사용해 데이터를 적재합니다.
    """
    if not json_data:
        print("적재할 데이터가 없습니다.")
        return

    try:
        # 1. JSON을 DataFrame으로 1차 변환 및 기본 정제
        df = pd.DataFrame(json_data)

        # 필요한 컬럼만 선택하고 간단한 이름으로 변경
        df = df[['PRD_DE', 'C1_NM', 'C2_NM', 'DT']]
        df.columns = ['base_date', 'province', 'item_name', 'value']

        # 2. '피벗(Pivot)'을 통해 데이터 구조를 wide format으로 변경
        #   - index: 행 기준이 될 컬럼 (날짜, 지역)
        #   - columns: 열 기준이 될 컬럼 (항목명)
        #   - values: 실제 데이터 값
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        wide_df = df.pivot_table(index=['base_date', 'province'],
                                 columns='item_name',
                                 values='value').reset_index()

        # 3. DB 테이블에 맞게 컬럼명 변경 및 최종 정제
        column_mapping = {
            '총지수': 'total_index',
            '신선식품': 'fresh_food_index',
            '신선어개': 'fresh_fish_index',
            '신선채소': 'fresh_vegetable_index',
            '신선과실': 'fresh_fruit_index',
            '신선식품제외': 'excluding_fresh_food_index'
        }
        wide_df.rename(columns=column_mapping, inplace=True)

        # base_date를 'YYYYMM'에서 'YYYY-MM-01' 형식의 날짜로 변환
        wide_df['base_date'] = pd.to_datetime(wide_df['base_date'], format='%Y%m').dt.date

        # 4. 정제된 데이터를 DB에 적재
        print(f"\n▶ '{table_name}' 테이블에 데이터 삽입을 시작합니다...")
        wide_df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"✅ '{table_name}' 테이블에 데이터 {len(wide_df)}건이 성공적으로 삽입되었습니다.")


    except Exception as e:
        print(f"!! 오류: 데이터 처리 또는 DB 적재 중 문제가 발생했습니다: {e}")