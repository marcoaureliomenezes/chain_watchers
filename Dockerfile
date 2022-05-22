FROM ubuntu

WORKDIR /app

COPY ./requirements.txt .

RUN apt-get update && apt-get install nodejs -y
RUN apt-get install python3-pip -y

RUN pip install --upgrade "pip==22.0.4" && \
    pip install -r requirements.txt

RUN pip install eth-brownie

COPY ./src .


CMD ["sleep", "infinity"]