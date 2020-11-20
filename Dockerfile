FROM mysql:5.7.32
FROM mongo:4.4.1-bionic

WORKDIR /usr/src/app

ADD mysql_sample ./mysql_sample

RUN mysql-client < ./mysql_sample/sakila-db/sakila-schema.sql
RUN mysql < ./mysql_sample/sakila-db/sakila-data.sql

CMD ["python", "main.py"]

COPY . .