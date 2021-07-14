ifneq (,$(wildcard ./dev.env))
    include dev.env
    export
endif

pg-init:
	docker run --name postgres12 -p 5432:5432 -e POSTGRES_USER=${USER} -e POSTGRES_PASSWORD=${PASS} -d postgres:12-alpine

pg-evoke:
	docker start postgres12

pg-createdb:
	docker exec -it postgres12 createdb --username=${USER} --owner=${USER} ${DB}

pg-dropdb:
	docker exec -it postgres12 dropdb ${DB}

my-init:
	docker run --name mysql8 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=${PASS} -d mysql:8

my-evoke:
	docker start mysql8

my-createdb:
	docker exec -it mysql8 mysql -uroot -p${PASS} -e "create database ${DB};"

my-dropdb:
	docker exec -it mysql8 mysql -uroot -p${PASS} -e "drop database ${DB};"


.PHONY: pg-init pg-createdb pg-dropdb my-init my-createdb my-dropdb
