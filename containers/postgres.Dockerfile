FROM postgres:9.5.10
WORKDIR /db
COPY ./dumps /db
