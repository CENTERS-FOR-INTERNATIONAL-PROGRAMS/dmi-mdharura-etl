## Deployment Guide

https://supabase.com/docs/guides/self-hosting/docker

<!-- https://supabase.com/docs/guides/self-hosting/docker -->

docker build -t mdharura_etl . -f Dockerfile.etl --no-cache


Install sqlfmt, to format sql.
https://docs.sqlfmt.com/getting-started/installation
pip3 install "shandy-sqlfmt[jinjafmt]" --break-system-packages