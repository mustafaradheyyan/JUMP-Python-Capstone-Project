import json
import pandas as pd
import requests

file_path = 'group_project/android-games.csv'

android_csv = pd.read_csv(file_path)
android_json = android_csv.to_json()

url = "https://mfcp436giirb7jwlvgizni2w7m0agrmc.lambda-url.us-west-1.on.aws/"
headers = {'SignatureHeader': 'XYZ', 'Content-type': 'application/json'}
payload = json.dumps({'type': 'payment-succeeded'})
querystring = {'myCustomParameter': 'squirrel'}

r = requests.post(url=url, params=querystring, data=android_json, headers=headers)
print(r)