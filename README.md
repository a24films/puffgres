Puffgres is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.

## Getting Started

Install puffgres by cloning the repo. You can use the `Justfile` to install, by running `just install` in the directory. (If you don't already have `just`, you can install it [here](https://github.com/casey/just))

TODO: Upload to package registries and/or have install script, i.e. `curl https://labs.a24films.com/puffgres-install.sh | sh` 

## Initializing

Navigate to the root level of your repo and run `puffgres init`. This will generate a `puffgres/` folder, complete with Dockerfile, and initial setup files. 

`puffgres.toml` defines several configuration variables, like the batch_size, set of retries, replay interval, etc. It also sets your environment variable paths - later paths override earlier ones. Our config looks like this, which works both in production and in dev:

`environment_files = ["../.env", "../.env.development", "./.env"]`

Here's the relevant environment variables to set:

- `DATABASE_URL` - non-pooled URL for your Postgres database, i.e. `postgresql://XYZ`
- `OTEL_EXPORTER_OTLP_ENDPOINT` - endpoint if want observability. We use Sentry, and this looks like `https://a123.ingest.us.sentry.io/api/1234/integration/otlp`
- `OTEL_EXPORTER_OTLP_HEADERS` - for us also from Sentry, i.e. `x-sentry-auth=sentry sentry_key=a123`
- `PUFFGRES_STATE_PATH` - the filesystem path for where your SQLite DB will live. Locally, this doesn't really matter / we have as `'./puffgres-state.db'`, but when you deploy, you'll want to put this on a persistent volume 
`TURBOPUFFER_API_KEY` - self-explanatory
`TURBOPUFFER_NAMESPACE_PREFIX` - this will prefix all of your namespaces. i.e. if you set this to `PUFFGRES_PRODUCTION` and make a namespace called `internal_film`, this would save it in turbopuffer as `PUFFGRES_PRODUCTION_internal_Film`
- `TURBOPUFFER_REGION` - standard, i.e. `aws-us-east-1`

You may also want to set environment variables you use in transformations, i.e. `ZEROENTROPY_API_KEY` for embeddings or similar. 


## Creating your first config

The primitive of Puffgres is a config, which defines a mapping between a Postgres table an a turbopuffer namespace. Run `puffgres new {mapping_name}` to create one. If you ran `puffgres new internal_film` you'd get a new directory like this:

`1772230207731_internal_film/`
- `config.toml`
- `transform.ts`


The folder's title has a timestamp of when the config was created. 

The `config.toml` lists the mapping configuration like this:

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

The namespace defines the destination in turbopuffer. If you try to create a config that references an already-used namespace, it will throw an error. 

The animating principle here is that a config is defined once, immutably. This will, on first run, backfill, routing all existing database rows through the `transform.ts` function. After this is complete, it will start a Change Data Capture loop — every Postgres change will flow through the transform loop, whether it be deleting, updating, or adding a row. 

At a high level, `transform.ts` will look like:

```
  const output: Action[] = input.map((event) => {
    if (event.operation === "delete") {
      return { type: "delete", id: event.id };
    }

    const [, buyerName, buyerId] = event.columns;
    const vector = embeddings[embeddingIdx++];

    return {
      type: "upsert",
      id: Number(buyerId),
      document: {
        buyer_name: buyerName,
      },
      vector,
      distance_metric: "cosine_distance",
    };
  });
  ```

In essence, `puffgres` takes all of the changed database rows, pipes them via `stdin` / `stdout` to the TypeScript process, and then we manipulate them. In many cases, this will mean generating embeddings, but it also might just be passing data through, filtering, or doing other transformations. The programmming model is left deliberately flexible; in the future, we may add typing or deliberately restrict it someway.

We structure configs/transforms to be immutable, because we don't want to developers to accidentally change a config midway through replication, leaving a turbopuffer namespace that doesn't reflect the code they have live. To enforce this, we hash the transform / config code, so that we'll throw an error and fail if we try to deploy different logic, much like a database migration. 


If you want to change how a config functions, you will need to create a new namespace. You can stop updating an old namespace, i.e. if you aren't still using it, by adding a tombstone. If you run `puffgres tombstone {config_name}` it will generate a `tombstone.toml` in that folder, and the CDC loop will ignore it. 


## Applying your first config

When you deploy puffgres in production, on startup it will apply all new configs, like a migration.

You may want to test this before you apply, you can use:

`puffgres dry-run`

which will pull a row from the database as defined in the config and push it through your transform + log it out 

In order to apply the config to your local database, you can run 

`puffgres apply` 

which will create some notion of these configs in the datbase. `puffgres apply` adds all of the configs in code into SQLite so that they are picked up for replication. This means you shouldn't really apply until you are ready; if you change a transform or config after this point, you'll get an error, because the canonical version is hashed. 

From here you can run 

`puffgres backfill` to run the backfill, or `puffgres run` which runs the whole change data capture loop, beginning with the backfill. 

If you mess up your local SQLite DB and want to start from scratch, you can use `puffgres reset`. A common workflow is to run `puffgres reset && puffgres setup && puffgres apply && puffgres run` which will start the CDC loop. 


## Deploying


To run this in in Railway, you should first create a new service, open to your repo, pointed with the root directory as `puffgres/`.

