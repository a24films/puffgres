Puffgres is a logical replication service that keeps Postgres entities mirrored in turbopuffer. Rather than duplicating application code every time you modify a vector (and risking partial successes that keep data out of sync), your Postgres changes automatically update.


## Getting Started

Eventually, install logic will work with an install script, i.e. `curl https://labs.a24films.com/puffgres-install.sh | sh`, or a Docker image. 

In the interim, or if you want to contribute to the Source, you should:
- insall [Rust](https://rust-lang.org/tools/install/) 
- install [Just](https://github.com/casey/just)
- clone the repo
- run `just install` on the repo. 

## Design principles

A bit of Puffgres' design philosophy:

- **You should not need to make two sets of database calls to keep entities upgraded in turbopuffer.** Such logic is bound to drift (forgetting to add parallel updates in turboopuffer / on every possible modification path) or have hard-to-detect failures (i.e. Postgres call succeeds, turbopuffer calls fails). Everything in turbopuffer should be derived from updates to Postgres.
- **Configs simply map changes on Postgres to transformations**. In essence, configs are mapping that show us what TS processes to route Postgres changes and backfills to.
- **Configs and transforms are immutable**. There's an abundance of thorny cases that come from letting us change a mapping, such as upserting different dimensional vectors to turbopuffer, or having some of our rows in turbopuffer transformed one way, and others transformd another way. Instead, we should always either "tombstone" an old config and create a new one, when we need to make a change. This makes applicaiton logic quite simple, involving simply changing the turbopuffer namespace we reference. 
- **The service should be able to stop or start at any time**. We should not rely on continuous execution for eventual consistency. The Postgres publication will continue notifying us of changes if we do not acknowledge previous ones; as such, we should only acknowledge when we are done processing.
- **State lives in separate SQLite db**. Puffgres is designed around separating replication from the operations of the primary db. Keeping shared state in Postgres (which we did in an early working version) meant that rollbacks would also wipe Puffgres state, making it much harder to recover cleanly. There's just a few tables so we skipped an ORM / rusqlite was more than enough.
- **The service should abstract away thorny cases**. Users should not need to consider retries, failures, batching, resumption, handoff (i.e. transitioning from a backfill to active logical replication, and not missing any rows in the transition). 
- **Transformations should be stateless, have no side effects, and be totally composed in TS**. Userspace should be written in a maximally usable and accessible way, in TypeScript. Functions should simply assume they will get a batch of rows to upsert to turbopuffer (batched so we can bulk embed/upsert) and only need to handle the translation. This also lets us use the whole scope of excellent pre-existing libraries, and the TS ecosystem, for tokenization, embedding, etc. 



## Initializing

Navigate to the root level of your repo and run `puffgres init`. This will generate a `puffgres/` folder, complete with Dockerfile, and initial setup files. 

`puffgres.toml` defines several configuration variables, like the batch_size, set of retries, replay interval, etc. It also sets your environment variable paths - later paths override earlier ones. Our config looks like this, which works both in production and in dev:

`environment_files = ["../.env", "../.env.development", "./.env"]`

Here's the relevant environment variables to set:

- `DATABASE_URL` - non-pooled URL for your Postgres database, i.e. `postgresql://XYZ` 
- `TURBOPUFFER_API_KEY` - self-explanatory
- `TURBOPUFFER_NAMESPACE_PREFIX` - this will prefix all of your namespaces. i.e. if you set this to `PUFFGRES_PRODUCTION` and make a namespace called `internal_film`, this would save it in turbopuffer as `PUFFGRES_PRODUCTION_internal_Film`
- `TURBOPUFFER_REGION` - standard, i.e. `aws-us-east-1`
- `OTEL_EXPORTER_OTLP_ENDPOINT` - endpoint if want observability. We use Sentry, and this looks like `https://a123.ingest.us.sentry.io/api/1234/integration/otlp`
- `OTEL_EXPORTER_OTLP_HEADERS` - for us also from Sentry, i.e. `x-sentry-auth=sentry sentry_key=a123`
- `PUFFGRES_STATE_PATH` - the filesystem path for where your SQLite DB will live. Locally, this doesn't really matter, and on Railway, we create/attach a volume called `puffgres-volume` and set this path to `/puffgres-volume/data/puffgres-state.db`.

You may also want to set environment variables you use in transformations, i.e. `ZEROENTROPY_API_KEY` or `BASETEN_API_KEY` for embeddings.


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
const input: Event[] = JSON.parse(readFileSync("/dev/stdin", "utf-8"));
const upsertEvents = input.filter((e) => e.operation !== "delete");
const buyerNames = upsertEvents.map(
  (e) => parseRow(e.columns).buyer_name ?? "",
);
const vectors = await embedBatchZeroEntropy(buyerNames);
const vectorMap = new Map(upsertEvents.map((e, i) => [e.id, vectors[i]]));

const output: Action[] = input.map((event) => {
  if (event.operation === "delete") {
    return { type: "delete", id: event.id };
  }

  const row = parseRow(event.columns);

  return {
    type: "upsert",
    id: event.id,
    document: {
      buyer_name: row.buyer_name,
    },
    vector: vectorMap.get(event.id)!,
    distance_metric: "cosine_distance",
    schema: {
      buyer_name: { type: "string" },
    },
  };
});
  ```

You'll note that `parseRow` function — this comes from looking at the schema of your Postgres table, and parsing the values as such. Puffgres also will generate a `schema.ts` (always autogenerated) so that your payloads are typed. If you forget to run this generation code, `puffgres check` will fail.

Here's what schema.ts looks like:

```
// Auto-generated by `puffgres generate`. Do not edit.
// Source: public.theatrical
import { parseRow as parseRowInternal, parseRows as parseRowsInternal, type Column } from "../../utils/puffgres";

export const columns = [
  { name: "id", type: "string" },
  { name: "buyer_name", type: "string" },
  { name: "buyer_id", type: "string" },
] as const satisfies readonly Column[];

export const parseRow = (cols: (string | null)[]) => parseRowInternal(columns, cols);
export const parseRows = (rows: (string | null)[][]) => parseRowsInternal(columns, rows);
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

If you mess up your local SQLite DB and want to start from scratch, you can use `puffgres reset`. A common workflow is to run `puffgres reset && puffgres apply && puffgres run` which will start the CDC loop.


## Deploying

To run this in in Railway, you should first create a new service, open to your repo, pointed with the root directory as `puffgres/`. You should set env variables and create a persistent volume where your SQLite DB will live.

We run `puffgres check` in CI. It won't catch immutability issues, because it doesn't have access to SQLite generally, but it will catch if you've generated a schema wrong or deployed an incorrect config. 


## Meta-notes

This originally came about to replace a hacky system we built internally, that kept a `turbopuffer_updated_at` column in Snowflake for some of our vector based tables. This meant a full table scan everytime our data pipeline ran (inefficient!), that it missed deletes, and that additions were quite slow. It also meant lots of duplicate code whenever we wrote a new transform, and easy regressions if we changed old code. 

I built a [very hacky](https://github.com/lucasgelfond/puffgres) version of this over a weekend, starting with a detailed spec and working through it with Claude. I then properly broke it up into PRs, we took it through code review (you can see the PRs / merge history on this repo!) and added testing, traditional CI, etc, plus lots of testing before we deployed and felt it was ready. We've been running puffgres in production internally for a little while now, without issue; I figured this was a generic enough problem it would be useful to release, in the Bryan Cantrill [primacy of toolmaking](https://www.youtube.com/watch?v=_GpBkplsGus) tradition. 

## Contributing 


Puffgres is divided into several crates:
- `state`, which manages Puffgres internal state in a SQLite database 
- `config`, which handles all of the mappings from Postgres tables to Turbopuffer namespaces
- `pg`, which sets up Postgres for logical replication and sets up the publication slot
- `replication`, which handles the actual change stream
- `core`, which routes new changes to their respective configs and deals with processing / retry logic
- `cli`, which provides the interface  

We are excited about PRs! We are not principally opposed to LLM-generated PRs, but they will be held to the same standard as traditional contributions, and we reserve the right to close low quality PRs at will. 


