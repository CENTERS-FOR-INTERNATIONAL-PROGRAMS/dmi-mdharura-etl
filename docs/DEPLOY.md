## Deployment Guide

https://supabase.com/docs/guides/self-hosting/docker

<!-- https://supabase.com/docs/guides/self-hosting/docker -->

docker build -t mdharura_etl_runner . -f Dockerfile.etl --no-cache
docker build -t mdharura_etl_webserver . -f Dockerfile.dagster --no-cache
docker build -t mdharura_etl_daemon . -f Dockerfile.dagster --no-cache


docker compose up -d --force-recreate --no-deps --build --no-cache


Install sqlfmt, to format sql.
https://docs.sqlfmt.com/getting-started/installation
pip3 install "shandy-sqlfmt[jinjafmt]" --break-system-packages