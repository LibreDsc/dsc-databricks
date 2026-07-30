# License

`dsc-databricks` is released under the MIT license. In practice that means
you can use it commercially, modify it, redistribute it and ship it inside a
closed-source product, provided the copyright notice and the license text
travel with any substantial portion of the software. There is no warranty.

The authoritative copy is [`LICENSE`][00] in the repository.

## MIT License

```text
MIT License

Copyright (c) 2026 LibreDsc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Third-party licenses

The released binary is statically linked and therefore ships its
dependencies inside the executable. The direct dependencies and their
licenses:

| Dependency | License |
| ---------- | ------- |
| [`github.com/databricks/databricks-sdk-go`][01] | Apache License 2.0 |
| [`github.com/LibreDsc/dsc-go-rdk`][02] | MIT |
| [`golang.org/x/text`][03] | BSD 3-Clause |

Each of those pulls in transitive dependencies of its own. The complete,
version-pinned set is [`go.mod`][04]; run `go mod download` and inspect the
module cache, or use a tool such as `go-licenses`, if you need a full
attribution report for redistribution.

## The documentation

The documentation on this site is part of the same repository and is covered
by the same MIT license. The site is built with
[Material for MkDocs][05], which is separately licensed under MIT.

<!-- Link references -->
[00]: https://github.com/LibreDsc/dsc-databricks/blob/main/LICENSE
[01]: https://github.com/databricks/databricks-sdk-go
[02]: https://github.com/LibreDsc/dsc-go-rdk
[03]: https://pkg.go.dev/golang.org/x/text
[04]: https://github.com/LibreDsc/dsc-databricks/blob/main/go.mod
[05]: https://squidfunk.github.io/mkdocs-material/
