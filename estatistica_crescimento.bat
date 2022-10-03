cd "C:\Program Files\PostgreSQL\13\bin"
psql -h 127.0.0.1 -p 5432 -d modelo_entidade  -U postgres  -c "select * from estatistica_crescimento();"
