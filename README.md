Puffgres is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.


## Getting Started

Install puffgres by cloning the repo. You can use the `Justfile` to install, by running `just install` in the directory. (If you don't already have `just`, you can install it [here](https://github.com/casey/just))

TODO: Upload to package registries and/or have install script, i.e. `curl https://labs.a24films.com/puffgres-install.sh | sh` 


## Initializing

Navigate to the root level of your repo and run `puffgres init`. This will generate a `puffgres/` folder, complete with Dockerfile, and initial setup files. Puffgres manages its own state in a SQLite DB, that you have running in production (and can have a local version), which is also created on init.

`puffgres.toml` defines several configuration variables, like the batch_size, set of retries, replay interval, etc. It also sets your environment variable paths - later paths override earlier ones. Our config looks like this, which works both in production and in dev:

`environment_files = ["../.env", "../.env.development", "./.env"]`


## Creating your first config

The primitive of Puffgres is a config, which defines a mapping between a Postgres table an a turbopuffer namespace. Run `puffgres new {mapping_name}` to create one. If you ran `puffgres new internal_film` you'd get a new directory like this:

`1772230207731_internal_film/`
- `config.toml`
- `transform.ts`

That first string is a timestamp of when the config was created. The TOML lists the mapping configuration. The transform defines logic for taking a batch of rows and turning them into corresponding turbopuffer rows (i.e. embedding, defining an upsert). 

A config looks like:

```
name = "internal_film"
namespace = "internal_film"

[source]
schema = "public"
table = "internal_film"

[id]
column = "id"
type = "string"
```

The namespace, of course, defines the destination in turbopuffer. You cannot have multiple configs which reference the same namespace; these will fail.

The idea here is that a config is defined once, immutably. Once you create and apply a config, we hash the transform/config code, so that if you change it in the future, it will fail. This is an intentional restriction; we don't want developers to accidentally change a config midway through replication, leaving a turbopuffer namespace that doesn't reflect their configs. 

Instead, if you change how you want to structure a config, you should:
- create a tombstone record (TODO) which will disable further change replication to a given namespace
- create a new config 

The transform takes in an event and returns it. The default set `transform.ts` file has a nice definition of how to process operations. 

TODO: clean this up, LOL

## Applying your first config

When you deploy puffgres in production, on startup it will apply all new configs, like a migration.

You may want to test this before you apply, you can use:

`puffgres dry-run`

which will pull a row from the database as defined in the config and push it through your transform + log it out 

In order to apply the config to your local database, you can run 


`puffgres apply` 

which will sync everything into the SQLite DB.

From here you can run 

`puffgres backfill` to run the backfill, or `puffgres run` which runs the whole change data capture loop, beginning with the backfill. 


If you mess up your local SQLite DB and want to start from scratch, you can use `puffgres reset`. 


## Deploying


To run this in in Railway, you should first create a new service, open to your repo, pointed with the root directory as `puffgres/`.

You should set relevant env variables, i.e.

DATABASE_URL
OTEL_EXPORTER_OTLP_ENDPOINT
OTEL_EXPORTER_OTLP_HEADERS
PUFFGRES_STATE_PATH
TOGETHER_API_KEY
TURBOPUFFER_API_KEY
TURBOPUFFER_NAMESPACE_PREFIX
TURBOPUFFER_REGION


Then, this should deploy when you push changes