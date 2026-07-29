# About Unity Catalog resource dependencies

This article explains how the Unity Catalog resources in this module relate
to each other, why some properties can only be set at creation, and how
grants converge. Understanding the chain helps you order configuration
documents correctly and predict what a change will — and will not — do.

## The securable hierarchy

Unity Catalog objects form a dependency chain that mirrors how managed
storage is authorized:

1. A **storage credential** holds a cloud identity — on Azure, an access
   connector's managed identity or a service principal — that can reach a
   storage account.
2. An **external location** binds a storage URL to a storage credential.
   Unity Catalog rejects locations that overlap an existing one.
3. A **catalog** stores managed data under a `storage_root` that must be
   covered by an external location, unless the metastore has default
   managed storage.
4. A **schema** lives in a catalog and may narrow storage with its own
   `storage_root`.
5. A **volume** lives in a schema. A `MANAGED` volume gets server-assigned
   storage; an `EXTERNAL` volume points at its own location.

**Connections** sit outside this chain: they hold federation endpoints for
foreign catalogs, and **service credentials** hold identities for external
services rather than storage.

In a configuration document, this chain is expressed with `dependsOn`: the
credential before the location, the location before the catalog, and so on
down to volumes and grants.

## Create-only properties

Several properties in the chain cannot converge in place. A catalog or
schema `storage_root` and a volume's `volume_type` and `storage_location`
are fixed at creation, because moving managed data between storage roots is
not an operation the platform offers. The resources therefore send these
properties only when creating; a later change in the configuration is
reported as drift by `test` but is not corrected by `set`. Changing such a
property for real means creating a replacement object and migrating data.

## Write-only secrets

A service principal's `client_secret` is accepted by the API but never
returned. The consequence is asymmetric: you can set it, but no operation
can verify it afterward. Exports omit it, and the credential resources
exclude it from comparison rather than reporting permanent drift.
Historically this is the same contract as Databricks secret values — write
often, read never.

## How grants converge

The Unity Catalog permissions API is a delta API: requests say what to add
and what to remove. The `Grant` resource turns that into declarative state
by treating one (securable, principal) pair as the unit of ownership. On
`set`, it reads the principal's current direct privileges, computes the set
difference against the desired list, and sends a single change with the
additions and removals. Unlike a raw API call, applying the same grant
twice changes nothing — the difference is empty.

The trade-off of this model is scope: the resource owns *all* of a
principal's direct privileges on the securable. Privileges granted outside
the configuration are removed on the next apply. Inherited privileges,
which flow down the hierarchy, are not touched.

## Where to go next

- [StorageCredential reference][01]
- [Catalog reference][02]
- [Grant reference][03]
- [How to export existing resources][04]

<!-- Link references -->
[01]: ../reference/resources/storage-credential.md
[02]: ../reference/resources/catalog.md
[03]: ../reference/resources/grant.md
[04]: ../how-to/export-resources.md
