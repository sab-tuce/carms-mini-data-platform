up:
	docker compose up -d --build

down:
	docker compose down -v

ps:
	docker ps

logs-api:
	docker logs -n 50 carms_api

logs-dagster:
	docker logs -n 50 carms_dagster_web

psql:
	docker exec -it carms_postgres psql -U carms -d carms

health:
	curl -s http://localhost:8000/health && echo

programs3:
	curl -s "http://localhost:8000/programs?limit=3" && echo
