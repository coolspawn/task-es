FROM alpine:3.12.0
RUN apk update && apk add python3 py3-pip bash curl
COPY ./requirements.txt /
RUN pip install -U pip setuptools wheel
RUN pip install -r /requirements.txt
COPY ./wait-for-es ./main.py ./generate_data.py ./verify.py /
CMD bash -c "./wait-for-es && python3 generate_data.py --elasticsearch-url http://es_7:9200 --index index1 && python3 main.py --elasticsearch-url http://es_7:9200 --index index2 && python3 -m pytest verify.py"
