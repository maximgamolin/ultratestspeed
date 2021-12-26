FROM pypy:3.8-buster
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
WORKDIR /code

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y netcat-openbsd gcc && \
    apt-get clean

COPY ./code /code
ADD ./containers/r.txt /code
RUN pip install -r r.txt
RUN pip install psycopg2cffi==2.9.0
