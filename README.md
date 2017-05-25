# Steps to run Configurator-exporter

## Clone deepInsights git repo
cd /opt/

git clone https://USERNAME@github.com/pramurthy/deepInsight.git

###### Note: replace USERNAME

## Set Pythonpath

export PYTHONPATH=$PYTHONPATH:/opt/deepInsight/configurator-exporter

## Run configurator server

cd /opt/deepInsight/configurator-exporter

python api_server.py -h
usage: api_server.py [-h] [-p PORT] [-i HOST]

optional arguments:

  -h, --help            show this help message and exit

  -p PORT, --port PORT  port on which configurator will listen, Default 8000

  -i HOST, --ip HOST    host ip on which configurator will listen, Default 0.0.0.0


python api_server.py -i 0.0.0.0 -p 8000




