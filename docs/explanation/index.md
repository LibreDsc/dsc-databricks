# Explanation

Explanation articles provide background and reasoning. They contain no
steps — read them to understand how the pieces fit together and why they
behave the way they do.

- [Why dsc-databricks is a trimmed Databricks CLI][04] — what the DSC engine
  demands of a resource, and why that rules out wrapping the official CLI.
- [What the fork keeps and drops][05] — the inventory, and where the line
  between this binary and the `dsc` engine falls.
- [About DSC v3 resources and this module][01] — the resource model,
  manifests, the `_exist` convention, and custom versus synthetic `test`.
- [About what-if predictions][02] — native projections, the `whatIfArg`
  mechanism, and the limits of prediction.
- [About Unity Catalog resource dependencies][03] — the securable chain,
  create-only properties, write-only secrets, and grant convergence.

<!-- Link references -->
[01]: about-dsc-v3-resources.md
[02]: about-what-if-predictions.md
[03]: about-unity-catalog-dependencies.md
[04]: about-the-databricks-cli-fork.md
[05]: what-the-fork-keeps-and-drops.md
