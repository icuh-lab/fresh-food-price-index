import requests
import json

def fetch_kosis_data(api_url: str) -> list | None:
    """
    KOSIS API를 호출하여 데이터를 JSON 형식으로 가져옵니다.

    Args:
        api_url (str): KOSIS API 요청 URL

    Returns:
        list | None: 성공 시 데이터 리스트, 실패 시 None
    """
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # 200 OK가 아닐 경우 예외 발생

        # KOSIS 응답이 비어있거나 오류 메시지를 포함하는 경우 처리
        data = response.json()
        if not data or "err" in data[0]:
            print("에러: API 응답에 문제가 있습니다.", data)
            return None

        print("API 데이터 수집 성공!")
        return data

    except requests.exceptions.RequestException as e:
        print(f"에러: API 호출 중 문제가 발생했습니다: {e}")
        return None
    except json.JSONDecodeError:
        print("에러: API 응답을 JSON으로 파싱할 수 없습니다.")
        return None