
test:
	docker-compose up --build --abort-on-container-exit --timeout 60 --exit-code-from test-app

es_start:
	docker-compose -f docker-compose_es.yml up --build -d

es_stop:
	docker-compose -f docker-compose_es.yml stop

es_stop:
	docker-compose -f docker-compose_es.yml down --remove-orphans