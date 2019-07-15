# Setting up the postgres db

## Install and run local posgres

```
./run_local_postgres_server.sh
```

## Create Schema

```
./pg.sh < schema.sql
```

## Create non-privileged users

```
./pg.sh < grants.sql	
./pg.sh < users.sql
```

## Create the tables

> This activates the virtual env
> and sources the *envs_secrets.sh* file

```
./run_create.sh
```

> You can tweak *envs_secrets.sh* and re-run
> for sample tables
