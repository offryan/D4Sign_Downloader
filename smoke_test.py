import time
import sys
import requests

print('smoke test: polling http://127.0.0.1:5000 ...')
for i in range(60):
    try:
        r = requests.get('http://127.0.0.1:5000', timeout=1)
        print('STATUS', r.status_code)
        print(r.text[:800])
        sys.exit(0)
    except Exception as e:
        time.sleep(0.25)
print('no response after timeout')
sys.exit(2)
