import PySimpleGUI as sg
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
    # Add some color
# to the window
    sg.theme('SandyBeach')	

# Very basic window.
# Return values using
# automatic-numbered keys
    layout = [
	[sg.Text('Please enter the information of the new seismic event')],
	[sg.Text('Time', size =(15, 1)), sg.InputText()],
	[sg.Text('latitude', size =(15, 1)), sg.InputText()],
	[sg.Text('longitude', size =(15, 1)), sg.InputText()],
    [sg.Text('depth', size =(15, 1)), sg.InputText()],
    [sg.Text('mag', size =(15, 1)), sg.InputText()],
    [sg.Text('magtype', size =(15, 1)), sg.InputText()],
    [sg.Text('nst', size =(15, 1)), sg.InputText()],
    [sg.Text('gap', size =(15, 1)), sg.InputText()],
    [sg.Text('dmin', size =(15, 1)), sg.InputText()],
    [sg.Text('rms', size =(15, 1)), sg.InputText()],
    [sg.Text('net', size =(15, 1)), sg.InputText()],
    [sg.Text('id', size =(15, 1)), sg.InputText()],
    [sg.Text('updated', size =(15, 1)), sg.InputText()],
    [sg.Text('place', size =(15, 1)), sg.InputText()],
    [sg.Text('type', size =(15, 1)), sg.InputText()],
    [sg.Text('horizontalerror', size =(15, 1)), sg.InputText()],
    [sg.Text('deptherror', size =(15, 1)), sg.InputText()],
    [sg.Text('magerror', size =(15, 1)), sg.InputText()],
    [sg.Text('magnst', size =(15, 1)), sg.InputText()],
    [sg.Text('status', size =(15, 1)), sg.InputText()],
    [sg.Text('locationsource', size =(15, 1)), sg.InputText()],
    [sg.Text('magsource', size =(15, 1)), sg.InputText()],
	[sg.Submit(), sg.Cancel()]
    ]

    window = sg.Window('Simple data entry window', layout)
    event, values = window.read()
    window.close()
    
    if event == "Cancel": return
    
    session = initialize_session()
    
    payload = {
                "time": [values[0]], "latitude": [values[1]], "longitude": [values[2]],
                "depth": [values[3]], "mag": [values[4]], "magType": [values[5]],
                "nst": [values[6]],"gap": [values[7]],"dmin": [values[8]],"rms": [values[9]],"net": [values[10]],"id": [values[11]],
                "updated": [values[12]],"place": [values[13]],"type": [values[14]],
                "horizontalError": [values[15]], "depthError": [values[16]],
                "magError": [values[17]],"magNst": [values[18]],"status": [values[19]],"locationSource": [values[20]],"magSource": [values[21]]
               }
   
    #for value in payload.values():
     #   print(value)
    r = session.post(url=URL, data=json.dumps(payload), headers={'Content-type': 'application/json'})
    print(r)

if __name__ == "__main__":
    main()