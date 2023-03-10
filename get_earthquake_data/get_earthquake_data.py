import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://p6rlsxtibledoazbo2ezhna22u0csmnx.lambda-url.us-west-1.on.aws/"

def initialize_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(URL, adapter)
    return session
    
def main():
    session = initialize_session()
    
    r = session.get(url=URL)
    print(r)
    print(r.text)

if __name__ == "__main__":
    main()