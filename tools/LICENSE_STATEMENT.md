# Statement: does any strong copyleft dependency reach this repository's source tree?

Date: 2026-09-16 (updated after pulling master)
Scope: `gsy-e-sdk` (this repository) only.
Author: dev@gridsingularity.com, assisted scan via `pip-licenses` + `tools/check_licenses.py`.

## Question

For the planned relicensing effort, does any strong-copyleft-licensed
third-party component reach source control (i.e. does any GPL/AGPL/SSPL/etc.
source end up committed or vendored into this repository), as opposed to
being used only as an external, separately-distributed tool?

## Finding

**No.** No strong-copyleft source code is vendored, copied, or otherwise
committed into this repository's source tree, and no strong-copyleft
package is installed as a real dependency either.

Basis for this conclusion:

1. **No vendoring.** No `vendor/`/`third_party/` directory and no embedded
   `LICENSE`/`COPYING` file were found in `gsy_e_sdk/`. Dependencies are
   resolved externally via `pip`/`requirements/*.txt` (plus an editable
   `git+https` install of `gsy-framework`), never copied into the git tree.

2. **`awesome-slugify` (GPLv3) and its dependency `Unidecode` (GPLv2+) are
   gone.** An earlier pass of this scan found `awesome-slugify` declared
   directly in `requirements/base.in` (the first line) and actively used in
   `gsy_e_sdk/redis_market.py`. After pulling `master`, commit `8695c2d`
   ("GSYE-934: Exchange awesome-slugify with python-slugify") has already
   fixed both sides: `requirements/base.in`/`base.txt` now pin
   `python-slugify==8.0.4`, and `redis_market.py` was updated to
   `from slugify import slugify; slugify(area_id)` — the `to_lower=True`
   kwarg was dropped, since `python-slugify`'s `slugify()` lowercases by
   default, so behavior is preserved. A clean install of `requirements/
   base.txt` + `requirements/dev.txt` in a fresh venv now shows neither
   `awesome-slugify` nor `Unidecode`.

3. **Swapping in `python-slugify` pulled in `text-unidecode` as a new
   transitive dependency**, whose `pip-licenses` output ("Artistic License;
   GNU General Public License (GPL); GNU General Public License v2 or
   later (GPLv2+)") is a classifier-concatenation artifact, not a real
   dual license — its actual declared `License` metadata field is
   `Artistic License` (permissive), confirmed with
   `pip-licenses --from=meta`. Same package/same non-issue as in `gsy-e`,
   `scm-engine`, and `gsy-web`. Exempted in
   `tools/license_exceptions.txt`. With this exemption, the gate passes:
   `OK: no strong copyleft licenses found among 77 packages.`

4. **`pylint` (GPL-2.0-or-later) is dev-only**, declared only in
   `requirements/dev.txt` (not `base.txt`), run as an external CLI, not
   imported by any `gsy_e_sdk` module. Exempted.

5. **Everything else is weak/file-level copyleft (LGPL, MPL):**
   `chardet` (LGPL, runtime), `astroid`, `paramiko` (LGPL, dev-only,
   `pylint`/`fabric3`), `certifi` (MPL). Weak copyleft's obligations attach
   only to modifications of the library itself, not code that merely
   imports/links it.

## Conclusion

No strong-copyleft source is vendored or committed into `gsy-e-sdk`'s own
source tree. The real, actively-used strong-copyleft runtime dependency
found in an earlier pass (`awesome-slugify` + `Unidecode`) has already been
removed on `master` (commit `8695c2d`), including the corresponding call-site
fix in `redis_market.py`. The CI gate (`tools/check_licenses.py`) now passes
cleanly, matching `gsy-e`'s, `scm-engine`'s and `gsy-web`'s results.
