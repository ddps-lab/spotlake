# azure_auth.py  Access Token 에러 해결법
### 발생 이유
- 2023년 2월 16일 20시 경 error발생 
  - /azure-collector/utill/azure_auth.py 의 21-22번라인
    ```python
    data = requests.post(f'https://login.microsoftonline.com/{realm}/oauth2/v2.0/token', data={'client_id': client_id, 'grant_type': 'refresh_token', 'client_info': '1',
                         'claims': '{"access_token": {"xms_cc": {"values": ["CP1"]}}}', 'refresh_token': refresh_token, 'scope': 'https://management.core.windows.net//.default offline_access openid profile'}).json()
    ```
    위 request의 response가 다음과 같았고
    
    ```python
    {
        ‘error’: ‘invalid_grant’,
        ‘error_description’: “AADSTS50076: Due to a configuration change made by your administrator, or because you moved to a new location, you must use multi-factor authentication to access ‘00000-0000-0000-0000-0000000000’.\r\nTrace ID: 00000-0000-0000-0000-0000000000\r\nCorrelation ID: 00000-0000-0000-0000-0000000000\r\nTimestamp: 2023-02-16 13:20:31Z”,
        ‘error_codes’: [50076],
        ‘timestamp’: ‘2023-02-16 13:20:31Z’,
        ‘trace_id’: ‘00000-0000-0000-0000-0000000000’,
        ‘correlation_id’: ‘00000-0000-0000-0000-0000000000’,
        ‘error_uri’: ‘https://login.microsoftonline.com/error?code=50076’,
        ‘suberror’: ‘basic_action’
    }  
    ```    
    그렇기에 24번라인의 다음 코드에서 에러가 발생

    ```python
    access_token = data['access_token'] 
    ```
  - 위 코드에서  access_token을 가져올 때 data에 access_token이 없어 발생
  
### 해결방법
1. 로컬에서 Azure CLI 로그인 (https://learn.microsoft.com/ko-kr/cli/azure/authenticate-azure-cli)
2. ~/.azure 폴더 msal_token_cache.json 파일에 access_token, refresh_token, client_id, realm의 정보가 있음
3. 해당 내용을 spotlake 계정 us-east-1 dynamodb AzureAuth 테이블의 해당 항목을 업데이트 하면 해결 가능