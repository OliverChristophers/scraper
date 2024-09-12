import requests
import time
import pandas as pd
import requests
import json
from datetime import datetime, timezone
import re
import pickle
from apscheduler.schedulers.background import BackgroundScheduler
from google.oauth2 import service_account



def inplay_good():
    cookies = {
        'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d': 'eyJpdiI6IlpxRlFVeG1vZUVtMGpKS3pNNFJwb2c9PSIsInZhbHVlIjoiY0hxWEdOS2VaRlhBeERDRk9yTk9qMTlXeGVJWmJkbGY3R2pjQVRKaWxlUUJPQjVENzlWQVFsMmtDTWQ5N0tnMXAyNzZDSncxaFl4ZEJkcnNSWU9saUM2UjVHRzIxWG9PTzNBb2UyaTB1OGxEekFUSzNDM2cydFlKY2xVNjI3MFpwc2lKRDMwdFZ5ZXhUVkk3WmM5ekFLOHBNSk9OdXFuck1ScFJCWEdYbHNkK2JyNDNUSmxiellwY0VkNGhoMC95ZHFhRVdjOCtwVndkZmpBVUVvaWs4TGluRitsYVIwc001TUVrL3cvdzE2ST0iLCJtYWMiOiJlMmM4MjFhYmQ2OGRiYjZkZTM0ZmUwZTFhMjg1YTg1M2E3ODQyN2VkNDNkN2Q1MDU4YzhhMGExYTEyYTFlZmZlIiwidGFnIjoiIn0%3D',
        'io': '4KHjYX6nGWhQDKUvADK9',
        'XSRF-TOKEN': 'eyJpdiI6ImRtRTBTcy9BTFV3NTgvRUdCWmxETEE9PSIsInZhbHVlIjoiVjVpSnRhZmpzV0YyV2h5eHZSTFQzN2pycGo3TWZMMXA5QVFtQ1hoOStwa2loRTVvcC9CMGZaWlYxbzZDSHBvMDBUZkkxeGIzUUgzRG9FdFZ0dDU0VWxEalJTRTlQSHJjc1dYYklERHZCcDZQZ05CeUpvUjNLYTJvVGpoaGRzR3QiLCJtYWMiOiJkZTlhZjg3MzE1ZjM0OWVmNGRiMGI4OTExMjNjZTNiMDU3ZWNkZWMyMDYyYWVjYWE0OWI2MjgxYjNkZjc3MDRkIiwidGFnIjoiIn0%3D',
        'session_inplayguru': 'eyJpdiI6ImtwZkhxeU1Pek5kZ1VCZ2JCYS9nV0E9PSIsInZhbHVlIjoiTy9xQ3psQ2xtT2gxTmRwczFhdldXWlVsWk5YMEp2SDFzWW1ROGllR01laXN5QWUzdnhZZC9JR0FNbGJNN2dWNUd2bmdKb2NFanBscjJNeVUrc2luNWp6NUgxSVYyb0RzdU1HSjRac3Iwd1ZIcXFYVzd4cGo4OWJyWGEzYW9EenciLCJtYWMiOiJmMWFlOGNhYTI5NGIzNjlkNjljODk5ZjdjNDQ4ZTYwM2JkNThjZDZkNjE2YjA2OTBmMTE5NTlkNjIwZGVkM2Q0IiwidGFnIjoiIn0%3D',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        # 'cookie': 'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=%3D; io=4KHjYX6nGWhQDKUvADK9; XSRF-TOKEN=eyJpdiI6ImRtRTBTcy9BTFV3NTgvRUdCWmxETEE9PSIsInZhbHVlIjoiVjVpSnRhZmpzV0YyV2h5eHZSTFQzN2pycGo3TWZMMXA5QVFtQ1hoOStwa2loRTVvcC9CMGZaWlYxbzZDSHBvMDBUZkkxeGIzUUgzRG9FdFZ0dDU0VWxEalJTRTlQSHJjc1dYYklERHZCcDZQZ05CeUpvUjNLYTJvVGpoaGRzR3QiLCJtYWMiOiJkZTlhZjg3MzE1ZjM0OWVmNGRiMGI4OTExMjNjZTNiMDU3ZWNkZWMyMDYyYWVjYWE0OWI2MjgxYjNkZjc3MDRkIiwidGFnIjoiIn0%3D; session_inplayguru=eyJpdiI6ImtwZkhxeU1Pek5kZ1VCZ2JCYS9nV0E9PSIsInZhbHVlIjoiTy9xQ3psQ2xtT2gxTmRwczFhdldXWlVsWk5YMEp2SDFzWW1ROGllR01laXN5QWUzdnhZZC9JR0FNbGJNN2dWNUd2bmdKb2NFanBscjJNeVUrc2luNWp6NUgxSVYyb0RzdU1HSjRac3Iwd1ZIcXFYVzd4cGo4OWJyWGEzYW9EenciLCJtYWMiOiJmMWFlOGNhYTI5NGIzNjlkNjljODk5ZjdjNDQ4ZTYwM2JkNThjZDZkNjE2YjA2OTBmMTE5NTlkNjIwZGVkM2Q0IiwidGFnIjoiIn0%3D',
        'origin': 'https://inplayguru.com',
        'priority': 'u=1, i',
        'referer': 'https://inplayguru.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }

    params = {
        'EIO': '4',
        'transport': 'polling',
        't': 'P1kTcLF',
    }
    
    print(f'retrieving io...')
    response = requests.get('https://ws.inplayguru.com/inplay/', params=params, cookies=cookies, headers=headers)
    dd = json.loads(response.text[1:])['sid']
    print(f'io: {dd}')
    


    cookies = {
        'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d': 'eyJpdiI6IlpxRlFVeG1vZUVtMGpKS3pNNFJwb2c9PSIsInZhbHVlIjoiY0hxWEdOS2VaRlhBeERDRk9yTk9qMTlXeGVJWmJkbGY3R2pjQVRKaWxlUUJPQjVENzlWQVFsMmtDTWQ5N0tnMXAyNzZDSncxaFl4ZEJkcnNSWU9saUM2UjVHRzIxWG9PTzNBb2UyaTB1OGxEekFUSzNDM2cydFlKY2xVNjI3MFpwc2lKRDMwdFZ5ZXhUVkk3WmM5ekFLOHBNSk9OdXFuck1ScFJCWEdYbHNkK2JyNDNUSmxiellwY0VkNGhoMC95ZHFhRVdjOCtwVndkZmpBVUVvaWs4TGluRitsYVIwc001TUVrL3cvdzE2ST0iLCJtYWMiOiJlMmM4MjFhYmQ2OGRiYjZkZTM0ZmUwZTFhMjg1YTg1M2E3ODQyN2VkNDNkN2Q1MDU4YzhhMGExYTEyYTFlZmZlIiwidGFnIjoiIn0%3D',
        'io':dd,
        'XSRF-TOKEN': 'eyJpdiI6Ilo4N0I0YkhPRHlYcUplNFhWR2hoY0E9PSIsInZhbHVlIjoiT3BZM01KRDdQdmxrUVc2d1FTNm1nZFIvMHplTjlKWGpTb1BzNnBscXZLUTduNFdIU21IVjJkWW10Wk1SSXBxTmtIMTNvRzVrRVhPSVpMdmpCL0p3R1BkMW1uT1dkN2lrV2FvcGp6UmpZTGRZa0xLbG9Pa0wybENFY0NBbWwzcVoiLCJtYWMiOiIyODQ4YTlkMWVjOWQzNjNlM2I4NzE0NjZiZTcxODM2MDY0NDRkNzA3M2YxNTMwODU3OWJjZjEwYjNiMDljNmU3IiwidGFnIjoiIn0%3D',
        'session_inplayguru': 'eyJpdiI6InNCaTQ3amxkSlFDY201UkppblNMcFE9PSIsInZhbHVlIjoieGxjVGtmNGxIeS91YjNOcXlHZ3NJM2d6MFhqTlJwb0hrblFFOEU1azNTUjRXWnN6Wkhpbjk0L2dMdEJkbmlNcGUzT3ZrWFh3amxwaTJDcGNwY2g4UHpZcjJuSHhhaTlFWFNtankxRDFVMjZ4bFpPb3JWSmhBY1lrWk1qV05JL1AiLCJtYWMiOiI3MTUyOGRkMzc5OTBhODU4ZWY4NDYyYWUxNWY4YTY3MTJmZjJjYjJlY2Y2YThiNDY1Y2Y5OTIwNWE4OTc5NjdjIiwidGFnIjoiIn0%3D',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'text/plain;charset=UTF-8',
        # 'cookie': 'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6IlpxRlFVeG1vZUVtMGpKS3pNNFJwb2c9PSIsInZhbHVlIjoiY0hxWEdOS2VaRlhBeERDRk9yTk9qMTlXeGVJWmJkbGY3R2pjQVRKaWxlUUJPQjVENzlWQVFsMmtDTWQ5N0tnMXAyNzZDSncxaFl4ZEJkcnNSWU9saUM2UjVHRzIxWG9PTzNBb2UyaTB1OGxEekFUSzNDM2cydFlKY2xVNjI3MFpwc2lKRDMwdFZ5ZXhUVkk3WmM5ekFLOHBNSk9OdXFuck1ScFJCWEdYbHNkK2JyNDNUSmxiellwY0VkNGhoMC95ZHFhRVdjOCtwVndkZmpBVUVvaWs4TGluRitsYVIwc001TUVrL3cvdzE2ST0iLCJtYWMiOiJlMmM4MjFhYmQ2OGRiYjZkZTM0ZmUwZTFhMjg1YTg1M2E3ODQyN2VkNDNkN2Q1MDU4YzhhMGExYTEyYTFlZmZlIiwidGFnIjoiIn0%3D; io=KoKBBNvVK3qbPU_VADSG; XSRF-TOKEN=eyJpdiI6Ilo4N0I0YkhPRHlYcUplNFhWR2hoY0E9PSIsInZhbHVlIjoiT3BZM01KRDdQdmxrUVc2d1FTNm1nZFIvMHplTjlKWGpTb1BzNnBscXZLUTduNFdIU21IVjJkWW10Wk1SSXBxTmtIMTNvRzVrRVhPSVpMdmpCL0p3R1BkMW1uT1dkN2lrV2FvcGp6UmpZTGRZa0xLbG9Pa0wybENFY0NBbWwzcVoiLCJtYWMiOiIyODQ4YTlkMWVjOWQzNjNlM2I4NzE0NjZiZTcxODM2MDY0NDRkNzA3M2YxNTMwODU3OWJjZjEwYjNiMDljNmU3IiwidGFnIjoiIn0%3D; session_inplayguru=eyJpdiI6InNCaTQ3amxkSlFDY201UkppblNMcFE9PSIsInZhbHVlIjoieGxjVGtmNGxIeS91YjNOcXlHZ3NJM2d6MFhqTlJwb0hrblFFOEU1azNTUjRXWnN6Wkhpbjk0L2dMdEJkbmlNcGUzT3ZrWFh3amxwaTJDcGNwY2g4UHpZcjJuSHhhaTlFWFNtankxRDFVMjZ4bFpPb3JWSmhBY1lrWk1qV05JL1AiLCJtYWMiOiI3MTUyOGRkMzc5OTBhODU4ZWY4NDYyYWUxNWY4YTY3MTJmZjJjYjJlY2Y2YThiNDY1Y2Y5OTIwNWE4OTc5NjdjIiwidGFnIjoiIn0%3D',
        'origin': 'https://inplayguru.com',
        'priority': 'u=1, i',
        'referer': 'https://inplayguru.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }

    params = {
        'EIO': '4',
        'transport': 'polling',
        't': 'P1kdgUp',
        'sid':dd,
    }

    data = '40'

    response = requests.post('https://ws.inplayguru.com/inplay/', params=params, cookies=cookies, headers=headers, data=data)
    print(response.text)




    cookies = {
        'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d': 'eyJpdiI6IlpxRlFVeG1vZUVtMGpKS3pNNFJwb2c9PSIsInZhbHVlIjoiY0hxWEdOS2VaRlhBeERDRk9yTk9qMTlXeGVJWmJkbGY3R2pjQVRKaWxlUUJPQjVENzlWQVFsMmtDTWQ5N0tnMXAyNzZDSncxaFl4ZEJkcnNSWU9saUM2UjVHRzIxWG9PTzNBb2UyaTB1OGxEekFUSzNDM2cydFlKY2xVNjI3MFpwc2lKRDMwdFZ5ZXhUVkk3WmM5ekFLOHBNSk9OdXFuck1ScFJCWEdYbHNkK2JyNDNUSmxiellwY0VkNGhoMC95ZHFhRVdjOCtwVndkZmpBVUVvaWs4TGluRitsYVIwc001TUVrL3cvdzE2ST0iLCJtYWMiOiJlMmM4MjFhYmQ2OGRiYjZkZTM0ZmUwZTFhMjg1YTg1M2E3ODQyN2VkNDNkN2Q1MDU4YzhhMGExYTEyYTFlZmZlIiwidGFnIjoiIn0%3D',
        'XSRF-TOKEN': 'eyJpdiI6ImZjUFZzZmlnVTY0ZkxZK0lBU2JMNFE9PSIsInZhbHVlIjoiN0dyQ2IxSVBROUdPUERmQ0dtMjJkeEIzUkV0V2owL3U3ZXVIMDBwNDZtWFppNDh6b1hKQkozQUZlNy9SbVhiVUVuNmx6VlZ4YUs2TnZrVEh0ZldaYWNUcHQ3dVcvanNLYXVHTHRGQU5VSHRzZzVZZWxRTHdNaUlJZDdZNGlpdXkiLCJtYWMiOiIxZjA4YjM3ZTMyN2U2MDZhYmEzZDY2MWRlYzQwZmQwM2M1M2M0MTNhYjJhMDAzOWRkMTQwYTQzMTM3MGRlYTNhIiwidGFnIjoiIn0%3D',
        'session_inplayguru': 'eyJpdiI6Im5BMGdndW5YNDlHU1V5REh5b1VqSVE9PSIsInZhbHVlIjoiSmsyK1Z1YWZla3d6UWxZWlZ3dytHMzNxVkd6a2thRDhZQkFGNWcyeENGMFdQakl2S2J2dTk0NTQyTVBkbXJpb2lIUG9SeFNlUnJ5REZrM1BzUnk4UCtFRFBncmkxRkk0NUg5THFia3BkbVliaEhBNVN3L2Zoanp0djVkTURnYXUiLCJtYWMiOiI5NjA3OGQxZTE5MTliMDU1NTUxYTNhZmRmYjE2NDAxODBmY2Y3MmEwZmFjZTVlODFhOWQ2YWMwNjIwMGQ1ODhjIiwidGFnIjoiIn0%3D',
        'io': dd,
    }

    response = requests.get(f'https://ws.inplayguru.com/inplay/?EIO=4&transport=polling&t=P1kbg92&sid={dd}', cookies=cookies)
    #print(response.text)



    pattern = r'\["session_start","([a-f0-9]{32})"\]'
    _match = re.search(pattern, response.text)
    session_start_id = _match.group(1) if _match else None



    headers = {
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'X-CSRF-TOKEN': 'aGI7rUCaqCxTupvk4X7C1tNIxf0cJQ5ZwYw4HKiP',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://inplayguru.com/',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Sync-Token': f'{session_start_id}',
        'sec-ch-ua-platform': '"Windows"',
    }
    
        
    print(f'inplayguru request with session id: {session_start_id}...')
    try:
        response = requests.get('https://ws.inplayguru.com/matches', headers=headers)
    except:
        print('retrying with TOR proxy...')
        try:
            response = requests.get('https://ws.inplayguru.com/matches', headers=headers)
        except:
            print('taking a rest...')
            time.sleep(10)
            print('retrying')
            response = requests.get('https://ws.inplayguru.com/matches', headers=headers)

    print(response.status_code)
    
    return response.json()



def diftime(time_str):

    # Parse the time string to a datetime object
    time_obj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")

    # Convert the time to UTC timezone
    time_obj = time_obj.replace(tzinfo=timezone.utc)

    # Get the current time in UTC
    current_time = datetime.now(timezone.utc)

    # Calculate the difference between the current time and the provided time
    time_difference = current_time - time_obj
    
    return time_difference
    
    
    

def d(data):
    #data = pickle.load(open('data.pkl', 'rb'))
    
    fin = list()
    ll = []
    for i in data:
        if 'min' in data[i][-1]['timer']:
            isfinished = True if data[i][-1]['timer']['min'] > 89 and data[i][-1]['timer']['min'] - 90 == data[i][-1]['timer']['ext'] else False
        
            if isfinished:
                fin.append(i)
                for g in data[i]:
                    ll.append(g)
      
    data = {i:data[i] for i in data if i not in fin}
    
    
    
    if ll:
        
        with open('data.pkl', 'wb') as file:
            pickle.dump(data, file)
            
            
        f = pd.DataFrame(ll)
        f = f.groupby('id').agg(lambda x: str(list(x))).reset_index()
        
        credentials = service_account.Credentials.from_service_account_file('static_files\ich-ds-5435ce21b563.json')

        # Step 5: Define your BigQuery dataset and table name
        table_id = 'ich-ds.odds.prod'

        # Step 6: Write the DataFrame to the BigQuery table
        f.to_gbq(destination_table=table_id, project_id=credentials.project_id, if_exists='append', credentials=credentials)

        print("DataFrame written to BigQuery successfully!")



def newrun(table_id, credentials):
    v = inplay_good()
    data = list()
    for i in v:
        data.append(v[i])
    
    f = pd.DataFrame(data)
    for column in f.columns:
        f[column] = f[column].apply(lambda x: str(x) if isinstance(x, dict) or isinstance(x, list) else x)

    f.to_gbq(destination_table=table_id, project_id=credentials.project_id, if_exists='append', credentials=credentials)

    return None
        
    


def run():
    
    data = pickle.load(open('data.pkl', 'rb'))
    start = time.time()
    v = inplay_good()
    for game in v:
        if game in data:
            data[game].append(v[game])
        else:
            data[game] = list()
            data[game].append(v[game])
    
    with open('data.pkl', 'wb') as file:
        pickle.dump(data, file)
    
    end = time.time()
    
    diff = end - start
    d(data=data)
    print(f'taken: {diff}')
    
    #sl = 59.3 - diff
    #f = sl/450
    
    #for _ in tqdm(range(450)):
    #    time.sleep(f)
        
    return None



    
    




if __name__ == '__main__':
    credentials = service_account.Credentials.from_service_account_file('ich-ds-5435ce21b563.json')

    table_id = 'ich-ds.odds.prod_odds'
    
    newrun(table_id, credentials)
    scheduler = BackgroundScheduler()
    
    scheduler.add_job(lambda: newrun(table_id, credentials), 'interval', seconds=60)

    scheduler.start()

    try:
        while True:
            pass
        
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()








