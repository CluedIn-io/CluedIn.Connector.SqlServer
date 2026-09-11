# Migrating a Connector to Multi-Version Targeting

This document tracks the migration of `CluedIn.Connector.SqlServer` from a single-version build to
the multi-version targeting pattern, part of a larger effort migrating CluedIn connectors and
enrichers. Reference docs consulted: `CluedIn.Connector.Dataverse.V2`,
`CluedIn.Enricher.GoogleMaps`, `CluedIn.Connector.AzureEventHubs`,
`CluedIn.Crawling.MasterDataServices` (all already migrated).

Branch: `feature/multi-version-targeting` (off `develop`).

---

## Overview

| CluedIn version | .NET TFM | Package suffix |
|---|---|---|
| 4.7.0 | net6.0 | `.470` |
| 4.8.0 | net6.0 | `.480` |
| 5.0.0-beta.* | net10.0 | `.500` |

4.6.0 excluded — no stream-repository/execution-context API usage in this connector's source that
would require it (grepped, none found); matches the exclusion default used across this migration
effort. `5.0.0-*` independently verified to resolve to `5.0.0-beta.576` against this repo's own
feeds.

---

## Step 1 — Pipeline template (`azure-pipelines.yml`)

Replaced `crawler.build.yml` steps-template (plus explicit `UseDotNet@2` 6.0 install and
`NuGetAuthenticate@1` step, and top-level `pool: windows-latest`) with `crawler.build.jobs.yml`
jobs-template. Switched pool to `ubuntu-22.04` to match every other migrated repo. Fixed
`pipelineTemplateRef`, which was hardcoded to `refs/heads/refactor` with no override parameter at
all — added the parameter, defaulting to `refs/heads/feature/multi-version-packaging`.

`runIntegrationTests` defaults to **`false`** — see Step 5, the integration test project doesn't
compile against current production code at all, unrelated to this migration.

---

## Step 2 — `Directory.Build.props`

Honours `CluedInMultiVersionTargetFramework` (net10.0 local fallback); derives
`CLUEDIN_V47`/`V48`/`V50` `DefineConstants`. No `LangVersion` issue hit here (unlike several
enrichers in this effort), but not explicitly pinned either — none of the source uses C# 11+ raw
string literals, so it wasn't needed.

---

## Step 3 — `Packages.props`

- `_CluedIn` guarded so the pipeline value wins.
- `CluedIn.Testing.Base` switched to the version-suffixed package ID
  (`CluedIn.Testing.Base.$(_CluedInPackageSuffix)`, i.e. `.470`/`.480`/`.500`) — verified all three
  exist on the feed before wiring up.
- `Microsoft.NET.Test.Sdk`, `AutoFixture.Xunit2`/`Xunit3`, `AutoFixture.AutoNSubstitute`,
  `xunit`/`xunit.v3`, `xunit.runner.visualstudio` split conditionally on `CLUEDIN_V50`, same pattern
  as every other migrated repo.
- **EF Core split by TFM** (net6.0 needs the last net6.0-compatible line; EF Core 8+ requires
  net8.0+): `Microsoft.EntityFrameworkCore`/`Microsoft.EntityFrameworkCore.InMemory` pinned to
  `6.0.16` for non-`CLUEDIN_V50`, `10.0.7` for `CLUEDIN_V50` (matching the AzureEventHubs doc's
  precedent exactly).
- **Pre-existing gap, unrelated to multi-version targeting:** `AutoFixture.Idioms` and
  `AutoFixture.AutoMoq` (referenced by the integration test project) had no `Update` entry in
  `Packages.props` at all — `dotnet restore` failed outright
  (`must have a version defined in Packages.props`) even before any of this migration's changes.
  Added pins (`4.18.1`, version-agnostic — neither package depends on a specific xunit generation).

### `CluedIn.DataStore` — real external blocker, not fixable from this repo

`CluedIn.DataStore` (referenced by both test projects) has **never been published for the 4.x
line** — verified directly: only a single floating version exists on the feed
(`5.0.0-beta.576`), and its package contents contain only `lib/net10.0/` (no multi-targeting at
all, unlike `CluedIn.Core`). This is the same category of blocker GoogleMaps' doc describes for
`CluedIn.Testing.Base`/`CluedIn.CrawlerIntegrationTesting` before those were migrated in their own
repos — except nobody has migrated `CluedIn.DataStore` yet.

Checked whether it's actually needed: no test source file imports the `CluedIn.DataStore` namespace
(only the unrelated `CluedIn.Core.DataStore`, from `CluedIn.Core` itself, is used), and
`CluedIn.Testing.Base.470`'s own nuspec has no dependency on it either. Safe to gate the
`PackageReference` itself to the 5.0 leg only
(`Condition="$(DefineConstants.Contains('CLUEDIN_V50'))"`) in both test csprojs — confirmed builds
clean on all three legs afterward.

---

## Step 4 — `NuGet.Config`

Renamed from `Nuget.config` (two-step `git mv`, Windows case-insensitivity workaround). Verified
directly: `CluedIn.Core` restores cleanly at `4.7.0` and `4.8.0` against the existing feeds
(`nuget.org`, `develop`, `release`, `AzurePipelines`) — no `public` feed needed, unlike
AzureEventHubs.

---

## Step 5 — Test projects

### Unit tests (`test/unit/Connector.SqlServer.Test`)

- Conditional `ItemGroup`s for xunit v2/v3 + AutoFixture generation selection, added directly to
  the csproj (not `test/Directory.Build.props`, avoiding the CS0433 clash the MasterDataServices
  doc describes).
- Added `GlobalUsings.cs` (`#if CLUEDIN_V50` picks `AutoFixture.Xunit3` vs `AutoFixture.Xunit2`) and
  removed the explicit `using AutoFixture.Xunit3;` from **6 test files** that had it directly
  (`SqlClientTests.cs`, `AutoNDataAttribute.cs`, and four files under `Utils/` — an initial grep
  only found one of the six due to a glob that didn't recurse into subdirectories; a rebuild
  surfaced the rest immediately as `CS0234`, fixed the same way).
- `CluedIn.DataStore` gated to the 5.0 leg (see Step 3).
- Verified clean on all three legs after fixes.

### Integration tests (`test/integration/Connector.SqlServer.Integration.Tests`) — pre-existing dead code, left disabled

This project is **not part of `CluedIn.Connector.SqlServer.sln`** and its restore failed outright
before any multi-version work even started (missing `Packages.props` entries — see Step 3). Once
restore was fixed, the actual test code in `SqlServerConnectorTests.cs` **fails to compile against
current production code at all** — it calls a `SqlServerConnector` constructor overload and a
`StoreData` overload that no longer exist (`CS1503`/`CS1501`), identically on **all three
multi-version legs including the plain local-dev default** (net10.0/5.0.0-*, no version override).
This proves it's not a version-compatibility issue this migration could fix — the test file is
simply out of sync with the current connector API and was, as far as can be determined, never
actually compiled by CI (explains why nobody noticed).

Fixing the test to match current production code is out of scope for a build-infrastructure
migration. `executeIntegrationTests` defaults to `false` — matching this project's real historical
state (never actually run), not a regression introduced here. The `PackageReference`/csproj changes
(xunit split, `CluedIn.DataStore` gating, EF Core pinning) are still in place so a future PR fixing
the actual test logic won't also have to redo this plumbing.

---

## Step 6 — Reset the semantic version (`GitVersion.yml`)

```yaml
next-version: 1.0
ignore:
  commits-before: 2026-06-25T00:00:00
```

Highest pre-existing tag is `4.5.3` at `2026-06-22T15:54:20+01:00`. Padded to
`2026-06-25T00:00:00` (2+ days past, per the local-time-parsing gotcha found earlier in this
effort). Verified with the pipeline's actual pinned `GitVersion.Tool 5.9.0`: resolves to
`FullSemVer: 1.0.0-multi-version-targeting.259` — confirmed `1.0.0`, correct.

---

## Checklist

- [x] `azure-pipelines.yml` — switched to `crawler.build.jobs.yml` with `multiVersionCluedInTargets` (4.7.0, 4.8.0, 5.0.0-beta.*); pool switched to `ubuntu-22.04`; `pipelineTemplateRef` parameterized (was hardcoded); `runIntegrationTests` defaults `false` (pre-existing broken integration tests)
- [x] `Directory.Build.props` — honours `CluedInMultiVersionTargetFramework`; `DefineConstants` derived
- [x] `Packages.props` — `_CluedIn` guarded; `CluedIn.Testing.Base` suffixed per leg; xunit v2/v3 split; EF Core pinned per TFM; `AutoFixture.Idioms`/`AutoFixture.AutoMoq` pins added (pre-existing gap); `CluedIn.DataStore` gated to 5.0 only (external blocker, never published for 4.x)
- [x] `NuGet.Config` — renamed from `Nuget.config`; verified sufficient feeds for 4.7.0/4.8.0
- [x] Unit tests — conditional xunit v2/v3 + AutoFixture selection; `GlobalUsings.cs` added; explicit `using AutoFixture.Xunit3;` removed from 6 files; builds clean on all three legs
- [x] Integration tests — restore-level gaps fixed, but left disabled by default: pre-existing, version-independent compile failure against current production API, out of scope to fix here
- [x] Source (`src/Connector.SqlServer`) — builds clean (0 errors) on all three legs, no `#if` guards needed
- [x] `GitVersion.yml` — `next-version: 1.0`; `ignore.commits-before: 2026-06-25T00:00:00`; verified `1.0.0` with pinned GitVersion.Tool 5.9.0
- [x] Push branch and confirm the actual Azure DevOps pipeline run is green end-to-end — PR #149, build 151999: all three legs + `Multi-version: publish` passed on the first push
