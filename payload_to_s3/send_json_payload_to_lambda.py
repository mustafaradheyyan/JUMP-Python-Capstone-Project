import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://mfcp436giirb7jwlvgizni2w7m0agrmc.lambda-url.us-west-1.on.aws/"

def initialize_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(URL, adapter)
    return session
    
def main():
    session = initialize_session()

    payload = {
                "time": ["2023-02-26T23:58:05.052Z"], "latitude": [41.805], "longitude": [79.8675],
                "depth": [10.0], "mag": [5.0], "magType": ["mb"],
                "nst": [46.0],"gap": [91.0],"dmin": [1.293],"rms": [0.8],"net": ["us"],"id": ["testingtesting"],
                "updated": ["2023-02-27T00:11:38.040Z"],"place": ["77 km NNW of Aksu, China"],"type": ["earthquake"],
                "horizontalError": [6.59], "depthError": [1.897],
                "magError": [0.078],"magNst": [52.0],"status": ["reviewed"],"locationSource": ["us"],"magSource": ["us"]
               }
    
    r = session.post(url=URL, data=json.dumps(payload), headers={'Content-type': 'application/json'})
    print(r)

if __name__ == "__main__":
    main()