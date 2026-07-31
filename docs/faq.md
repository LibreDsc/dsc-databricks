# Frequently asked questions

Most questions about `dsc-databricks` are really questions about where it
sits next to the tools you already use for Databricks. The answers below
are grouped by what you are trying to decide: which tool to reach for, how
these resources behave once you do, and what it takes to run them.

## Which tool fits

Databricks configuration can be described by more than one tool, and they
are not competing for the same job. A rough division:

| Tool | Fits best when |
| ---- | -------------- |
| [Declarative Automation Bundles][06] | You ship a project — jobs, pipelines, notebooks and the configuration around them — from source through environments. |
| [Databricks Terraform provider][07] | You provision Databricks together with the rest of your cloud estate and want one plan, one apply and one state file across all of it. |
| `dsc-databricks` | Workspace configuration belongs in an estate you already describe with Microsoft DSC, alongside operating system and application configuration. |

??? faq "Should I use this instead of Terraform or bundles?"

    Only if Microsoft DSC is already how you describe configuration.

    Bundles and the Terraform provider are the paths Databricks maintains,
    and both are more complete than this project: bundles understand the
    development lifecycle of a data project, and the Terraform provider
    covers close to the whole platform. If either already fits your
    workflow, there is no reason to move.

    What this project adds is a way to express Databricks workspace
    configuration in the same document, with the same verbs and the same
    what-if preview, as everything else you manage with the [Microsoft DSC
    engine][05] — Windows and Linux configuration, application settings,
    registry, packages. The value is the single description of a machine or
    an environment, not a better Databricks story on its own.

??? faq "Can I use it alongside Terraform or bundles in the same workspace?"

    Yes, provided each object has one owner.

    Nothing in `dsc-databricks` claims a workspace or fences it off. It
    reads and writes through the same public APIs, so it coexists with any
    other tool. The failure mode is shared ownership: if a catalog is
    declared in a Terraform configuration *and* in a DSC document, each
    apply will pull it toward its own definition and the two will keep
    reverting one another.

    Split by resource type or by workspace object rather than by team, and
    write the boundary down somewhere both sides read.

??? faq "There is no state file — how does it know what to change?"

    It asks the workspace, every time.

    Each operation reads the live state of one instance through the
    Databricks API and compares it to what you declared. Nothing is cached
    between runs, so there is no state file to store, lock, share or
    reconcile, and no possibility of the recorded state disagreeing with
    reality.

    The trade-off is the other side of the same coin: without a record of
    what it created, the tool cannot know that something it made last week
    has since disappeared from your configuration. See the question on
    deleting resources below.

??? faq "Does this replace the Databricks CLI?"

    No. It is deliberately useless for the things the CLI is good at.

    There is no interactive mode, no table output, no browsing your
    workspace, no running jobs or syncing files. The binary answers the DSC
    engine's questions about resources and changes the workspace when told
    to. Keep the official CLI installed for everything else —
    [What the fork keeps and drops][02] is the inventory.

??? faq "Why isn't this part of the official Databricks CLI?"

    It was proposed, and declined.

    [databricks/cli#4349][08] added a `databricks dsc` command with almost
    exactly this shape. Databricks declined it in January 2026, recommending
    the Terraform provider or Declarative Automation Bundles for declarative
    configuration, and suggesting that DSC integration could merit its own
    dedicated CLI. This project is that dedicated CLI.
    [Why dsc-databricks is a trimmed Databricks CLI][01] covers the
    technical reasons a fork was needed regardless.

??? faq "Is this an official Databricks product?"

    No. It is an independent open-source project, released under the MIT
    license with no warranty and no affiliation with Databricks. Support is
    what the [issue tracker][09] provides. Judge it accordingly before
    putting it on a production path.

## How the resources behave

??? faq "Will it delete resources that are not in my configuration?"

    No. Deletion is always something you ask for explicitly.

    A DSC apply only touches the instances your document names. To remove
    one, you declare it with `_exist: false` and the engine routes that to
    the resource's `delete` operation — absence is a state you write down,
    not something inferred from a resource going missing from a file.

    If you are used to a tool that removes what it no longer sees, this is
    the difference to keep in mind: an object you delete from your document
    is abandoned, not destroyed.

??? faq "What happens if someone changes a resource in the portal?"

    The next run sees it and reports it.

    Because state is read live, a manual change simply shows up as a
    difference: `dsc config test` reports the differing properties, and
    `dsc config set` moves the resource back to what you declared. There is
    no state file to become stale and no import step to repair the
    relationship — the workspace is the record.

??? faq "Can I preview changes before applying them?"

    Yes, and the prediction is computed by the resource itself.

    ```powershell
    dsc config set -w -f .\workspace.dsc.yaml
    ```

    Every resource implements what-if natively: it validates the input the
    same way `set` would, reads the current state, and projects the result
    without calling a single mutating API. What you get back is the state
    that `set` would produce, plus the properties that would change.
    [Preview changes with what-if][03] shows the workflow, and
    [About what-if predictions][04] explains where the prediction can and
    cannot be exact.

??? faq "Can I adopt resources that already exist?"

    Yes, and there is no import step.

    Since there is no state file, nothing has to be registered before a
    resource is managed: declaring an object that already exists simply
    makes the next `set` an update instead of a create. To get a starting
    document out of a workspace, `export` enumerates every instance of a
    type as state you can paste into a configuration:

    ```powershell
    dsc resource export -r LibreDsc.Databricks/Catalog
    ```

    See [Export existing resources][10].

??? faq "Are secret values written into the output?"

    No. Write-only properties are never read back.

    `string_value` on a `Secret` is accepted by `set` and never populated by
    `get`, because the API does not return it — so it cannot appear in
    result documents, in exported state or in a what-if prediction. The same
    holds for the other write-only fields, such as a storage credential's
    client secret.

    That is not encryption at rest for your configuration files: a secret
    you write literally into a DSC document is as exposed as the file is.
    Keep secrets in a variable or reference resolved at apply time.

## Coverage and running it

??? faq "Why are there no resources for jobs, pipelines or notebooks?"

    Because those are where bundles are genuinely the better answer.

    The resource set is chosen around durable configuration rather than
    transient activity — a cluster policy is a state, a job run is an event.
    Jobs, pipelines and notebooks come with a development lifecycle
    (source layout, environment targets, deployment, running) that
    [Declarative Automation Bundles][06] models directly and a
    property-comparison model does not.

    If your question is "how do I deploy my data project", reach for
    bundles. If it is "how do I keep this workspace's configuration the way
    I declared it", that is what these resources are for.

??? faq "Does it support Unity Catalog?"

    Yes. Catalogs, schemas, volumes, storage and service credentials,
    external locations, connections and grants each have a resource, and
    they can be declared in one document with the dependencies between them
    expressed in the usual DSC way.

    [About Unity Catalog dependencies][11] describes the ordering these
    objects require; the [resource reference][12] lists everything
    available.

??? faq "Do I need Windows or PowerShell?"

    No. Microsoft DSC v3 is cross-platform and so is this binary — Windows,
    Linux and macOS, on `amd64` and `arm64`. Configuration documents are
    YAML or JSON, and the engine is a standalone executable rather than a
    PowerShell module.

    PowerShell is convenient for *installing* the engine and appears
    throughout these docs for that reason, but nothing about running the
    resources depends on it.

??? faq "Which version of the DSC engine do I need?"

    Version 3.2 or later.

    Earlier engines do not understand how the manifests advertise what-if
    (a `whatIfArg` on the `set` method), so `dsc config set --what-if` will
    not be offered. Everything else works, but there is little reason to
    stay behind. [Installation][13] covers getting the engine.

??? faq "How does it authenticate, and does that work unattended?"

    Through the Databricks SDK, exactly as the official CLI does.

    There is no custom authentication code in this project at all.
    Environment variables, `.databrickscfg` profiles, Microsoft Entra
    service principals and the cloud-specific methods all behave the way
    they do everywhere else in the Databricks tooling.

    Unattended use was the design point: nothing prompts, nothing waits on a
    terminal, and anything that cannot be resolved from the environment
    fails with a diagnostic on stderr and a specific [exit code][14]. See
    [How to authenticate to Databricks][15].

??? faq "What if the resource I need doesn't exist yet?"

    Ask for it, or add it.

    Coverage grows by demand rather than by generating a resource per API,
    so an [issue][09] describing what you are trying to configure is useful
    input. Adding one is a contained job — a state struct, a handler, a
    registration line and tests — and [CONTRIBUTING.md][16] walks through
    it.

## Still stuck

Open an [issue][09] with what you were trying to declare and what came
back. Diagnostics from a run with `DSC_TRACE_LEVEL=DEBUG` set are the most
useful thing to attach.

<!-- Link references -->
[01]: explanation/about-the-databricks-cli-fork.md
[02]: explanation/what-the-fork-keeps-and-drops.md
[03]: how-to/preview-changes-with-what-if.md
[04]: explanation/about-what-if-predictions.md
[05]: https://learn.microsoft.com/powershell/dsc/overview
[06]: https://docs.databricks.com/aws/en/dev-tools/bundles/
[07]: https://registry.terraform.io/providers/databricks/databricks/latest/docs
[08]: https://github.com/databricks/cli/pull/4349
[09]: https://github.com/LibreDsc/dsc-databricks/issues
[10]: how-to/export-resources.md
[11]: explanation/about-unity-catalog-dependencies.md
[12]: reference/index.md
[13]: getting-started/index.md
[14]: reference/exit-codes.md
[15]: how-to/authenticate.md
[16]: https://github.com/LibreDsc/dsc-databricks/blob/main/CONTRIBUTING.md
