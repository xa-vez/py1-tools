##INSTAL##

cd ./cloe-device
virtualenv env
source env/bin/activate
pip install -r requirements.txt


##RUN##

source env/bin/activate
python cloe_mqtt_device.py


#READ MESSAGES PUBLISHED

