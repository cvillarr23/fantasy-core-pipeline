# Third-party dependencies

The [MIT License](LICENSE) covers this repository's code and documentation.
Dependencies retain their own licenses, including the exceptions below.
The Docker image preserves installed wheel license files under
`/root/.local/lib/python3.11/site-packages/*.dist-info/` and the CBC license
under `pulp/solverdir/cbc/`.

| Included component | License | Source and redistribution requirements |
|---|---|---|
| psycopg2-binary 2.9.9 | LGPL-3.0-or-later with an OpenSSL linking exception | [Source](https://github.com/psycopg/psycopg2/tree/2.9.9). Keep the license, supply corresponding source when distributing binaries, and preserve recipients' ability to replace/relink the library. The wheel also bundles native libraries; its Python metadata does not inventory their licenses. |
| certifi 2026.7.22 | MPL-2.0 | [Source](https://github.com/certifi/python-certifi). Preserve license notices and make the corresponding covered source, including any changes, available under MPL. |
| pathspec 1.1.1 | MPL-2.0 | [Source](https://github.com/cpburnz/python-pathspec). Installed through the Black development tool, which the current Dockerfile also includes. Preserve notices and covered source. |
| orjson 3.13.0 | MPL-2.0 AND (Apache-2.0 OR MIT) | [Source](https://github.com/ijl/orjson). Retain all bundled license texts; the MPL applies to included covered components. |
| tqdm 4.70.1 | MPL-2.0 AND MIT | [Source](https://github.com/tqdm/tqdm). The distinction between its code and documentation is explained in the wheel's `LICENCE`. Preserve that text. |
| PuLP 2.8.0's bundled CBC solver | EPL-1.0 | [CBC source](https://github.com/coin-or/Cbc). PuLP's Python code is MIT, but its solver binaries have a separate `coin-license.txt`. Preserve it and provide the corresponding solver source when distributing binaries. |

These versions describe the 2026-10-11 dependency check, not a transitive lock.
Repeat the check after dependency updates. Before distributing an image, archive
the corresponding sources for its exact covered binary versions, including
wheel-bundled libraries and operating-system packages; upstream URLs alone do
not replace source-delivery obligations.

The cross-repository [licensing guide](https://github.com/cvillarr23/fantasy-project/blob/main/docs/contributing/licensing.md)
records the audit scope, commands and remaining image-level checks.
