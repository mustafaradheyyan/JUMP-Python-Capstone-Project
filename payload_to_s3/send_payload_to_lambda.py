import json
import pandas as pd
import requests

file_path = 'group_project/cleaned_global_earthquake_data_10.csv'

earthquake_csv = pd.read_csv(file_path)
earthquake_json = earthquake_csv.to_json()

url = "https://mfcp436giirb7jwlvgizni2w7m0agrmc.lambda-url.us-west-1.on.aws/"
headers = {'SignatureHeader': 'XYZ', 'Content-type': 'application/json'}
payload = json.dumps({'type': 'payment-succeeded'})
querystring = {'myCustomParameter': 'squirrel'}

r = requests.post(url=url, params=querystring, data=earthquake_json, headers=headers)
print(r)