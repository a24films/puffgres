## State

Puffgres retains its own state, independent of restarts of the service, in a separate schema called `puffgres` on the Postgres table. It holds things such as: hashed versions of the configs and transforms (to ensure they don't change), streaming checkpoints (so we know where on the WAL to begin listening), backfill progress, and a "dead letter" queue of rows that failed that we will want to retry in the future.

We have a model of branching off of our prod database for user dev databases. Because of this setup, databases mirroring main will also pull the up-to-date puffgres state, and only resume streaming from the most recent point. 

## Deploys 

In our deploy pipeline, we have a `puffgres apply` that takes new, committed files and applies them to state and then we run the `puffgres run` loop. It's no problem if we have some gap where the puffgres streaming loop isn't running, because it is designed to handle interrupts and eventual consistency. 

We provide the Dockerfile that we use. In essence, you should commit the generated `puffgres` folder that comes from `puffgres init` inside of your project, and developers should be able to add configs at will / change configuration. The puffgres service itself will pull from our canonical repo, and run based on the files in your repo. 

## CI

Because puffgres' typed schema used for transforms is reliant on Postgres column order, we neeed to re-generate schema whenever there is a database migration on a table we're listening to. `puffgres check` will generate schemas for all of your watched tables, and compare them to the generated schemas, throwing an error if they are out of sync. 

## Observability

We support OpenTelemetry for tracing and metrics. This works great with Sentry's Logs product, which is how we monitor the service. To do this, you just need to set `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS`. See [Advanced options](./advanced-options.md#environment-variables) section for more.
