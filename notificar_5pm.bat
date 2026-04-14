@echo off
set PATH=C:\Users\sofianoguera_habi\AppData\Local\Programs\Python\Python312;C:\Users\sofianoguera_habi\AppData\Local\Programs\Python\Python312\Scripts;%PATH%
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sofianoguera_habi\AppData\Roaming\gcloud\application_default_credentials.json

cd /d C:\Users\sofianoguera_habi\agenda-visitas
python notificar.py 5pm >> notificar.log 2>&1
