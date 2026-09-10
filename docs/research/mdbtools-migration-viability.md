# Is mdbtools a viable replacement for our SQLAlchemy/pyodbc Access migration?

**Date:** 2026-09-10
**Question:** Our `convert` pipeline reflects an Access `.accdb` with SQLAlchemy +
`sqlalchemy-access` + `pyodbc`, which is Windows-only. `dpmcore` instead shells out to GNU
`mdbtools`. Can mdbtools drive *our* pipeline, or does adopting it mean losing the metadata
our model generator depends on?

---

## Verdict

**Yes — adopt architecture (a): use mdbtools as a metadata + data source on Linux, build the
SQLite with SQLAlchemy as we do today, and keep reflection where it already is (on the
SQLite).** This is a real recommendation, not a hedge. Two findings drive it:

1. **Our model generator never touches Access.** `schema/main.py:106` (`sqlite_to_schema`)
   reflects the **SQLite**, and the `schema` CLI command takes a `.sqlite` path
   (`cli.py:252-279`). Access reflection exists only inside `migrate/`. So "reflection moves
   downstream, off Access" — the premise of option (a) — is not a change we need to make. It
   is already the architecture. The question reduces to the much narrower one of whether
   mdbtools can supply what `migrate/processing.py` needs.

2. **Empirically, on the real DPM database, it supplies all of it.** Against
   `DPM 2.0_withOperations_3.5.accdb`, mdbtools 1.0.1 gave **63/63 tables, 369/370 columns
   (the 1 difference is a genuine 3.2→3.5 rename), 63/63 primary keys, 100% type agreement
   after our existing name-based recovery, 180/180 foreign keys, and 0 NULL-fidelity
   violations across 226 NOT NULL columns and 2.7M rows.** I built a working SQLite from
   mdbtools output, ran the real `schema_to_sqlalchemy` generator over it, and queried the
   result through the generated ORM with multi-hop relationship traversal.

The author's hypothesis — that `type_registry.py`'s name-based rules close the fidelity gap
left by CSV export — is **correct, and stronger than expected**: the rules are *exhaustive*
for this schema, not merely lucky (see [Finding 3](#finding-3)).

### Recommendation, concretely

- **(a) — adopt.** mdb-schema for structure, `mdb-export -0` for data, MSysRelationships for
  FKs, then `MetaData` → `create_all` → existing `schema` command unchanged.
- **(c) — the fallback split (dpmcore's model) is worth keeping as a transitional step**, but
  it is *not* the destination. Two code paths means two fidelity profiles to test forever.
  If (a) validates on a second release, delete the pyodbc path.
- **(b) — do not write a SQLAlchemy dialect over mdbtools.** Ruled out empirically, not on
  taste: the mdbtools ODBC driver's `SQLPrimaryKeys` and `SQLForeignKeys` are **empty stubs**
  (`src/odbc/odbc.c:255-322`), and its `SQLColumns` reports no column sizes and wrong
  nullability. A dialect would see *less* metadata than the CLI already gives us. See
  [§6](#6-could-a-sqlalchemy-dialect-sit-on-top).

### The honest caveats, up front

- **mdbtools silently corrupts dates after ~2700 AD into `1900-01-00`** — 11 rows in this
  database. DPM uses `9999-12-31` as an open-ended sentinel. This is a real bug in current
  upstream and needs an explicit workaround. See [Finding 4](#finding-4).
- I **could not produce a same-release pyodbc baseline** (impossible on Linux), so the
  strongest possible comparison — mdbtools 3.5 vs pyodbc 3.5 — was not run. See
  [§Open questions](#open-questions-and-what-i-could-not-determine).
- The installed 0.7.1 **cannot open `.accdb` at all**. Everything positive above is 1.0.x.

---

## How to read the evidence in this document

| Marker | Meaning |
|---|---|
| **[EMPIRICAL]** | I ran this on this machine against the real DPM `.accdb`. Command and real output shown. |
| **[SOURCE]** | Read from mdbtools source at a cited file:line. |
| **[DOC]** | From documentation, package indexes, or upstream manifests, with URL. |

Scratch dir: `/tmp/mdbtools-research`. Nothing in `/home/jim/opendpm` was modified except this
file; `/home/jim/dpm/assets/` was not modified.

---

## Version matters enormously: 0.7.1 vs 1.0.x

Everything in this report distinguishes these. They are not the same tool for our purposes.

**[EMPIRICAL] The preinstalled 0.7.1-6build1 cannot open the DPM database at all:**

```
$ mdb-ver "/home/jim/dpm/assets/DPM 2.0_withOperations_3.5.accdb"
Unknown Jet version.
Error: unable to open file /home/jim/dpm/assets/DPM 2.0_withOperations_3.5.accdb

$ mdb-tables "…3.5.accdb"
Unknown Jet version.
Couldn't open database.
```

The file is ACE, not Jet:

```
$ xxd -l 32 "…3.5.accdb"
00000000: 0001 0000 5374 616e 6461 7264 2041 4345  ....Standard ACE
00000010: 2044 4200 0300 0000 b56e 0362 6009 c255   DB......n.b`..U
```

Version byte at `0x14` is `0x03`. **[SOURCE]** `include/mdbtools.h.in:89-95` defines
`MDB_VER_ACCDB_2010 = 0x03`, and `src/libmdb/file.c:158-166` accepts
`MDB_VER_ACCDB_2007` … `MDB_VER_ACCDB_2019`. That enum does not exist in 0.7.1.

**[EMPIRICAL] I built 1.0.1 from the release tarball locally (no sudo, `--prefix=/tmp/...`)
and it opens the file cleanly:**

```
$ /tmp/mdbtools-research/local/bin/mdb-ver "…3.5.accdb"
ACE14
$ mdb-tables -1 "…3.5.accdb" | wc -l
63
```

63 tables — exactly the table count in `3.2-sample-original.sqlite`.

### Flag availability gap (all **[EMPIRICAL]**)

| Flag | 0.7.1 | 1.0.x | Why it matters |
|---|---|---|---|
| `-0 / --null=char` | `invalid option -- '0'` | works | **The flag that makes NULL distinguishable from `''`.** Without it, nullability cannot be inferred. |
| `-T / --datetime-format` | `invalid option -- 'T'` | works | Fixes the ambiguous default `MM/DD/YY`. |
| `-B / --boolean-words` | `invalid option -- 'B'` | works | `TRUE`/`FALSE` instead of `0`/`1`. |
| `-b hex` | strip\|raw\|octal only | adds `hex` | Irrelevant here — DPM has no binary columns. |
| `mdb-schema` backend | `-I <backend>` | positional arg | Breaks any script written for the other. |

**[SOURCE]** `--null` has existed since `v0.9.4` (`git show v0.9.4:src/util/mdb-export.c`
line 76 contains the `"null"` option). So the practical floor is 0.9.4; realistically 1.0.0+,
which is what every supported distro ships ([§5](#5-availability)).

Note this bites `dpmcore` itself: `services/export_csv.py:132` passes `-T`, which **0.7.1
rejects outright**. Their code requires 1.0.x whether or not that is documented.

---

## What our pipeline actually consumes from Access

This is the crux, so it is worth being precise. Reading `migrate/processing.py` and
`migrate/transformations.py`, `schema_and_data()` uses Access reflection for exactly:

| Needed from Access | Where | Notes |
|---|---|---|
| table names, column names | `reflect_schema` → `schema.reflect()` | |
| column types (genericized) | `genericize`, `processing.py:36-38` | `as_generic()` — Access-specific types are **deliberately discarded** |
| primary keys | `table.primary_key` (`processing.py:76`) | |
| declared FKs | reflection, then heavily augmented | see below |
| row data | `connection.execute(select(table))` | |

And, decisively, these are **not** taken from Access:

- **Nullability is data-derived, not declared.** `processing.py:84-85`:
  `column.nullable = column in nullable_columns`, where `nullable_columns` is populated in
  `transformations.py:36-38` only when an actual `None` is seen. Access's `NOT NULL` is
  discarded.
- **Indexes are discarded outright.** `processing.py:74`: `table.indexes.clear()`.
- **Enums are data-derived**, via `parse_rows` + `CheckConstraint` (`processing.py:80-81`).
- **~34 FKs are synthesized by us, not read from Access** —
  `add_foreign_keys_to_table` and `heal_cross_table_foreign_keys`.

So the metadata surface mdbtools must cover is narrow: **names, types, PKs, declared FKs, and
row data.** Nullability, indexes, and enums are already our own inference, and are indifferent
to where the bytes came from.

---

## The five load-bearing findings

### Finding 1 — Structure is recovered essentially perfectly

**[EMPIRICAL]** Table sets, compared against `3.2-sample-original.sqlite`:

```
mdbtools tables: 63 | groundtruth: 63
=== only in mdbtools(3.5) ===      (empty)
=== only in groundtruth(3.2) ===   (empty)
=== in both: 63 ===
```

Column sets (from `mdb-export` header rows across all 63 tables):

```
mdbtools cols: 370 | groundtruth cols: 370
=== only in mdbtools(3.5) ===    TableGroup.ParentTableGroupID
=== only in groundtruth(3.2) === TableGroup.ParentGroupID
```

**369/370 identical.** The single difference is a column rename between releases 3.2 and 3.5.
Attributing it to release rather than tool is a judgement, but a safe one: mdbtools reads the
name out of the file and has no mechanism to rename a column.

Primary keys, after loading mdb-schema's sqlite DDL and reflecting with our own
`reflect_tables`: **63/63 tables agree on the exact PK column tuple.**

### Finding 2 — mdb-schema's *postgres* backend is far richer than its *sqlite* backend

This is the most actionable surprise, and it inverts the obvious choice.

**[SOURCE]** `src/libmdb/backend.c:131-146`, the sqlite type map:

```c
[MDB_REPID]   = { .name = "INTEGER" },   /* GUID → INTEGER (!) */
[MDB_NUMERIC] = { .name = "INTEGER" },   /* precision and scale destroyed */
[MDB_MONEY]   = { .name = "REAL" },
[MDB_TEXT]    = { .name = "varchar" },   /* note: no needs_char_length → length lost */
```

versus `backend.c:85-100`, postgres: `MDB_REPID → UUID`, `MDB_NUMERIC → NUMERIC(p,s)`,
`MDB_TEXT → VARCHAR(n)`, `MDB_BOOL → BOOLEAN`.

**[EMPIRICAL]** The same table, both backends:

```
-- sqlite backend
CREATE TABLE `Concept` (
	`ConceptGUID`   INTEGER NOT NULL,      <-- wrong
	`ClassID`       INTEGER NOT NULL,
	`OwnerID`       INTEGER
	, PRIMARY KEY (`ConceptGUID`)
);

-- postgres backend
CREATE TABLE IF NOT EXISTS "concept" (
	"conceptguid"   UUID NOT NULL,          <-- correct
	"classid"       INTEGER NOT NULL,
	"ownerid"       INTEGER
);
```

Ground truth agrees with postgres on intent: `"ConceptGUID" CHAR(32) NOT NULL`.

**[EMPIRICAL]** Full type histogram from the postgres backend shows lengths and defaults are
preserved, and even an autonumber column is detected:

```
    182 INTEGER          25 VARCHAR (20)      7 TIMESTAMP WITHOUT TIME ZONE
     51 UUID             22 VARCHAR (255)     7 BOOLEAN DEFAULT 1
     24 BOOLEAN DEFAULT 0  16 TEXT            4 BOOLEAN DEFAULT FALSE
     14 VARCHAR (50)      4 DOUBLE PRECISION  1 SERIAL
     … VARCHAR (30|3|200|100|1|8|10)
```

The `access` backend is better still for our purposes — native Access type names, original
identifier case (postgres lowercases everything, **[SOURCE]** `backend.c:373-388` registers
`to_lower_case`):

```
CREATE TABLE [Category] (
	[CategoryID]        Long Integer NOT NULL,
	[Code]              Text (20) NOT NULL,
	[Description]       Memo/Hyperlink (255),
	[IsEnumerated]      Boolean NOT NULL DEFAULT 1,
	[RowGUID]           Replication ID
);
```

**Practical consequence: do not use `mdb-schema … sqlite`.** Use the `access` backend for
types and identifier case, and the `postgres` backend (or MSysRelationships) for FKs.

**[EMPIRICAL]** The complete native-type inventory of this database is only 7 types:

```
183 Long Integer   51 Replication ID   16 Memo/Hyperlink   4 Double
 74 Text           35 Boolean           7 DateTime
```

**There is no Currency, Numeric/Decimal, Binary, OLE, Byte, Single, or 16-bit Integer.** So
the two worst sqlite-backend defects — `NUMERIC → INTEGER` losing precision/scale, and
`MONEY → REAL` — are real bugs that **do not apply to DPM at all**. Worth restating plainly:
the theoretical fidelity risk here is much larger than the actual risk *for this database*,
and that gap is load-bearing for the recommendation. It would need rechecking if EBA ever
introduces a decimal or currency column.

### Finding 3 — Our name-based type recovery is exhaustive, not lucky

`type_registry.py:26-59` recovers UUID/Date/Boolean from column names because *SQLite* loses
them. That machinery turns out to cover precisely what *mdbtools* loses too.

**[EMPIRICAL]** I built a SQLite from `mdb-schema … sqlite` DDL (all 63 `CREATE TABLE`s valid,
loaded via `executescript`: 63 tables, 71 indexes), then reflected it with our real
`reflect_tables` + `detect_types` and compared to ground truth through `sql_to_data_type`:

```
TYPES: agreement 369/369 = 100.0%
PRIMARY KEYS: agreement 63/63 = 100.0%
```

Not a collapse to `text` — the distributions are identical and diverse:

```
mdbtools-derived(3.5): {'integer':183,'text':90,'boolean':35,'uuid':51,'real':4,'date':7}
groundtruth(3.2):      {'integer':183,'text':90,'boolean':35,'uuid':51,'real':4,'date':7}
```

`Concept.ConceptGUID` arrives as `INTEGER` in the DDL and comes out of `detect_types` as
`Uuid()`. Why this is robust rather than fortunate:

```
$ # all 51 Replication ID columns
      4 ConceptGUID
     47 RowGUID
$ # how many do NOT end in "guid"?
0
$ # all 7 DateTime columns
Date, FromReferenceDate(2), FromSubmissionDate, PublicationDate, ToReferenceDate(2)
$ # all 35 Boolean columns
Has*(5) Is*(28) ParentFirst UseIntervalArithmetics
```

Every GUID column ends in `GUID`; every datetime column ends in `Date`; every boolean starts
`Is`/`Has` **except exactly `ParentFirst` and `UseIntervalArithmetics`** — which are precisely
the two hardcoded exceptions at `type_registry.py:44`. The rules were evidently derived from
this schema and cover it completely.

### Finding 4 — Two real mdbtools bugs; one is benign, one is not

**(4a) GUIDs are malformed but losslessly repairable.** **[EMPIRICAL]**

```
$ mdb-export "…3.5.accdb" Release
1,"3.4","02/06/24 00:00:00",,"released",0,"{083F7C98-D0CF-4D87-A2E19DC464712242}"
```

Segment lengths are **8-4-4-16** — the fourth hyphen is missing. **[SOURCE]**
`src/libmdb/data.c:953-968`: the default format is `MDB_BRACES_4_2_2_8`, set at
`file.c:133`. The correct `MDB_NOBRACES_4_2_2_2_6` format exists and **the ODBC driver uses
it** (`src/odbc/odbc.c:112`), but **no CLI tool calls `mdb_set_repid_fmt`** — so `mdb-export`
always emits the broken form.

All 32 hex digits are present, and our ground truth stores bare lowercase hex, so
`re.sub(r'[^0-9a-f]','',s.lower())` lands exactly on our current representation. Byte order is
correct — I verified this by set intersection against ground truth rather than assuming:

```
raw intersection      : 3
swapped mdb ∩ gt      : 0
mdb ∩ swapped gt      : 0
full-reverse mdb ∩ gt : 0
```

Raw beats every endianness variant, so the ordering is right. (Only 3 of 15,350 overlap
because `3.2-sample-original.sqlite` is a *sample* of a different release — but 3 exact
32-hex-char collisions cannot be chance, which is what makes this a valid ordering check.)

**(4b) Dates after ~2700 AD are silently corrupted. This is the one genuine blocker.**

**[EMPIRICAL]** 11 rows across the database:

```
$ grep -ho '"[0-9]\{4\}-[0-9]\{2\}-00 [0-9:]*"' csv/*.csv | sort | uniq -c
     11 "1900-01-00 00:00:00"
```

`1900-01-00` is not a valid date — day zero. **[SOURCE]** `src/libmdb/data.c:891-897`:

```c
void
mdb_date_to_tm(double td, struct tm *t)
{
	…
	if (td < 0.0 || td > 1e6) // About 2700 AD
		return;
```

The function is `void` and **returns without writing anything**. Its caller
(`data.c:939-951`) does `struct tm t = { 0 };` then `strftime`, so an out-of-range value
strftimes an all-zero `tm` → `1900-01-00 00:00:00`, with **no error and no warning**. Access
dates are days since 1899-12-30, so `9999-12-31` is 2,958,465 — well over the `1e6` guard.

DPM uses `9999-12-31` as its open-ended sentinel: ground truth has
`ModuleVersion.FromReferenceDate = '9999-12-31'` for `IF_TM`, exactly where mdbtools reports
`1900-01-00`. **[SOURCE]** The guard is still present in current upstream `HEAD` (commit
`cc4aa5d`), so this is **not fixed in 1.0.1 or in git**.

This is not theoretical harm. It broke my loader:

```
sqlalchemy.exc.IntegrityError: NOT NULL constraint failed: ModuleVersion.FromReferenceDate
```

Access declares that column `NOT NULL`, so you cannot null the corrupted value — you must
either write a wrong date or violate the declared constraint. **Any adoption of mdbtools must
detect `1900-01-00` explicitly and map it to `9999-12-31`.** Silent acceptance would corrupt
the sentinel that DPM uses to mean "no end date", which downstream consumers compare against.

### Finding 5 — FKs survive completely, once you know where to look

The sqlite backend emits **zero** FKs; postgres emits **148**. **[SOURCE]** This is by
design, not a bug: `backend.c:403` registers sqlite without `MDB_SHEXP_RELATIONS`, while
postgres/mysql/oracle include it.

**[EMPIRICAL]** Comparing the 148 against ground truth's 180:

```
=== in BOTH: 146 ===
=== only mdbtools: 2 ===   (both MSysNavPane* system tables)
=== only groundtruth: 34 ===
```

So **every one of the 146 user-table FKs mdbtools reports is correct.** The 34 it misses
decompose exactly:

**11 are relationships Access marks "do not enforce"** (`grbit & 0x02` =
`dbRelationDontEnforce`). **[SOURCE]** `backend.c:845-852` emits a comment instead of a
constraint. **[EMPIRICAL]**:

```
$ mdb-export -H -q '' "…accdb" MSysRelationships | awk -F, '{print $2}' | sort | uniq -c
    146 0
     11 2
      2 4352
```

These 11 are still fully readable — `MSysRelationships` is exposed via `mdb-tables -S` /
`mdb-export`, and reading it directly recovers **157 of 180** ground-truth FKs.

**The remaining 23 are all `rowguid → concept.conceptguid`** — which is not reflection at all,
it is `transformations.py:82`:
`add_foreign_key_to_table(table, "RowGUID", "Concept.ConceptGUID")`, applied to every table by
our own code.

**[EMPIRICAL]** Running the real pipeline confirms the arithmetic closes:

```
tables=63 cols=370 tables_with_pk=63
FKs from MSysRelationships: 156
FKs total after transformations.py: 181     (ground truth: 180)
```

**FK metadata is not a loss at all.** It is *better* than the sqlite backend suggests, and our
existing synthesis rules cover the residue. Note this also means we would gain something: the
`grbit=2` non-enforced relationships are visible to us as *typed metadata* for the first time.

---

## Detailed answers to the specific questions

### 1. `mdb-schema` DDL fidelity

**[EMPIRICAL]** Options in 1.0.1: `--table`, `--namespace`, `--drop-table`, `--not-null`,
`--default-values`, `--not-empty`, `--comments`, `--indexes`, `--relations`. Backends
(**[SOURCE]** `backend.c:331-410`): `access`, `sybase`, `oracle`, `postgres`, `mysql`,
`sqlite`.

| Aspect | sqlite backend | postgres backend | access backend |
|---|---|---|---|
| Column types | lossy (see Finding 2) | rich | native Access names |
| VARCHAR length | **lost** | preserved | preserved |
| Precision/scale | **lost** (`→INTEGER`) | `NUMERIC(p,s)` | `Numeric(p,s)` |
| `NOT NULL` | yes | yes | yes |
| `DEFAULT` | yes | yes | yes |
| PRIMARY KEY | yes, inline | **no** | no |
| FOREIGN KEY | **no** | **yes (148)** | no |
| Indexes | yes | yes | "not implemented" |
| Identifier case | preserved | **lowercased** | preserved |
| Autonumber/counter | `INTEGER` | `SERIAL` | `Long Integer` |

**Quality of the sqlite output:** syntactically clean — all 63 `CREATE TABLE`s plus 71
indexes loaded via `sqlite3.executescript` with zero errors. Semantically it is the weakest
backend. **Caveat found the hard way: `--no-indexes` also suppresses PRIMARY KEY** in the
sqlite backend, because **[SOURCE]** PKs are emitted by `mdb_print_pk_if_sqlite`
(`backend.c:580-613`) which is gated with index emission. My first run produced 0 PKs for this
reason.

*(One earlier apparent failure — 2 `CREATE INDEX` statements failing with "no such table" —
was my own naive `split(';')` skipping statements that followed `--` comment lines, not an
mdbtools ordering problem. With `executescript` everything loads.)*

### 2. `mdb-export` type handling

**Everything is text, but types are recoverable and NULL is unambiguous.** The critical
question — can NULL be distinguished from empty string — is **yes, in 1.0.x, via `-0`**:

```
$ mdb-export "…accdb" Release                      # Description is NULL
1,"3.4","02/06/24 00:00:00",,"released",0,"{…}"    # renders as empty — ambiguous
$ mdb-export -0 '\N' -B "…accdb" Release
1,"3.4","02/06/24 00:00:00",\N,"released",FALSE,"{…}"   # unambiguous
```

**This is the single most important flag for us**, because `transformations.py:36-38` derives
nullability from observing `None`. Without `-0` that inference is impossible; with it, it is
exact.

**[EMPIRICAL] NULL fidelity is perfect.** Against the 226 columns Access declares `NOT NULL`,
across 2.7M loaded rows:

```
Access declares NOT NULL on 226 of 370 columns
columns declared NOT NULL but containing NULL after mdb-export load: 0
columns declared NULLable that contain no NULLs (tightened by data-derived pass): 20/144
```

Zero spurious NULLs, and the data-derived tightening pass still finds 20 columns to harden.

Per-type behaviour (all **[EMPIRICAL]**):

- **Booleans:** `0`/`1` by default; `-B` gives `TRUE`/`FALSE`. Access's internal `-1` is
  normalised away — no `-1` appears.
- **Dates:** default is **ambiguous `MM/DD/YY`** (`02/06/24`). `-T '%Y-%m-%d %H:%M:%S'` gives
  ISO. This is exactly the problem `dpmcore` papered over with
  `_DATE_FORMAT_TABLES = ("Release",)`.
- **GUIDs:** malformed 8-4-4-16, losslessly repairable (Finding 4a).
- **Memo/long text:** faithful. Long DPM-XL expressions with commas and braces survive intact,
  correctly quoted. Note memo values contain embedded newlines, so `wc -l` overcounts rows
  (2,697,437 lines vs 2,697,114 real rows) — use a real CSV parser, which handles them
  correctly. `-e/--escape-invisible` exists if you need single-line output.
- **Currency/decimal:** untestable here — DPM contains none.
- **Binary:** DPM contains none (0 `BYTEA` columns). **Warning: `-b hex` mangles GUID
  columns** — it hex-encodes the already-formatted GUID string. Verified:
  `bytes.fromhex('7B3038…7D').decode()` → `{083F7C98-D0CF-4D87-A2E19DC464712242}`. Do not
  pass `-b hex` on this database.
- **Fixed-width padding:** `Property.PeriodType` yields `'stock               '` (20 chars).
  **This is source data, not an mdb-export artifact** — ground truth has the identical padded
  value at length 20. No regression, but it does mean enum detection sees padded/unpadded/case
  variants either way.

**Robustness and speed [EMPIRICAL]:** all 63 tables exported with **zero non-zero exit codes
and zero bytes on stderr**, 2.7M rows, in **~9-15 seconds** for a 281 MB `.accdb`.

### 3. Relationships, FKs, indexes

Covered in Finding 5. Summary: **FKs fully recoverable** (146 enforced from
`mdb-schema … postgres`, all 159 rows including the 11 non-enforced readable directly from
`MSysRelationships`, remaining 23 synthesized by our own `transformations.py`). MSys tables are
accessible (`mdb-tables -S` lists `MSysObjects`, `MSysRelationships`, `MSysACEs`,
`MSysQueries`, …).

**Indexes:** emitted by the sqlite/postgres/mysql/oracle backends, but **irrelevant to us** —
`processing.py:74` clears them deliberately. So "indexes are not implemented for access/sybase"
costs us nothing.

**On the README's constraint caveat** (`README.md:302`, "some referential integrity constraints
may not be fully enforced due to cyclic dependencies"): this is unchanged by mdbtools. The
mdbtools-derived database reproduces it exactly — I hit the same
`SAWarning: Cannot correctly sort tables; there are unresolvable cycles between tables
"Concept, Organisation"`. Same behaviour today, same behaviour after.

### 4. `.accdb` (ACE) vs `.mdb` (Jet 4)

- **0.7.1: no ACE support. Fatal.** DPM files are `.accdb`, and 0.7.1 rejects them at the magic
  string (see above). This alone disqualifies the preinstalled version.
- **1.0.x: ACE supported.** **[SOURCE]** `mdbtools.h.in:89-95` covers ACCDB 2007→2019;
  `file.c:158-166` dispatches on them. **[EMPIRICAL]** the real 281 MB DPM `.accdb` opens
  cleanly, reports `ACE14`, and every table reads with no errors.
- **No encryption/compression problems encountered.** The file is not password-protected. I did
  not test an encrypted or compacted `.accdb`; 1.0.0 NEWS mentions
  `write-encrypted-pages` work, but I did not verify it.

### 5. Availability

**[DOC]** Distro versions — **the 0.7.1 problem is essentially historical**:

| Distro | mdbtools version | Source |
|---|---|---|
| Ubuntu 22.04 LTS (jammy) | `1.0.0+dfsg-1` | [packages.ubuntu.com](https://packages.ubuntu.com/search?keywords=mdbtools) |
| Ubuntu 24.04 LTS (noble) | `1.0.0+dfsg-1.2ubuntu1` | ditto |
| Ubuntu 25.10 / 26.04 LTS | `1.0.1-0.1` / `1.0.1-0.1build1` | ditto |
| Debian 12 (bookworm) | `1.0.0+dfsg-1.1` | [packages.debian.org](https://packages.debian.org/search?keywords=mdbtools) |
| Debian 13 (trixie) | `1.0.1-0.1` | ditto |
| Debian 11 (bullseye, oldoldstable) | `0.9.1-1` | ditto |
| Homebrew (macOS + Linux) | **1.0.1**, bottled for Apple Silicon + Intel + Linux ARM64/x86_64 | [formulae.brew.sh](https://formulae.brew.sh/formula/mdbtools) |

**[EMPIRICAL]** This machine is the outlier, not the norm:

```
$ cat /etc/os-release  → Ubuntu 20.04.6 LTS (Focal Fossa)
$ apt-cache policy mdbtools → Installed: 0.7.1-6build1  (focal/universe)
```

Focal is the last release shipping 0.7.1, and its standard support ended April 2025. **Every
currently-supported Debian/Ubuntu ships 1.0.x**, all of which have `.accdb` support, `-0`,
`-T`, and `-B`. The version gap that looked fatal at the start of this investigation is a
property of this dev box, not of the ecosystem. It is still worth pinning a minimum version
check (`mdb-ver --version`) and failing loudly, because 0.7.1 fails in *confusing* ways
("Unknown Jet version").

**Windows:** no official mdbtools build. Available via MSYS2/Cygwin or WSL. This matters only
if we wanted mdbtools to be the *sole* path; for a Linux/macOS path it is moot.

**GitHub Actions runners — [DOC], from `actions/runner-images` manifests:**

- `ubuntu-24.04` ([Ubuntu2404-Readme.md](https://raw.githubusercontent.com/actions/runner-images/main/images/ubuntu/Ubuntu2404-Readme.md)):
  **mdbtools absent. unixODBC also absent.** Needs `sudo apt-get install -y mdbtools`
  (→ 1.0.0, sufficient).
- `macos-15` ([macos-15-Readme.md](https://raw.githubusercontent.com/actions/runner-images/main/images/macos/macos-15-Readme.md)):
  **mdbtools absent**, but Homebrew 6.0.18 is present, so `brew install mdbtools` → 1.0.1
  (bottled, so fast).

**Compare this to today's cost.** `migrate-database.yml:23-65` runs on `windows-latest` and
must download and silently install the **Microsoft Access Database Engine** — a pinned
third-party MS installer (`accessdatabaseengine_X64.exe`, SHA256-verified, cached):

```yaml
runs-on: windows-latest
…
ACCESS_ENGINE_URL: https://download.microsoft.com/download/3/5/C/…/accessdatabaseengine_X64.exe
ACCESS_ENGINE_SHA256: "04e96c9f1a1f7d251a88aececf1dc10ff65950392787427c00814a43308003de"
…
Start-Process -FilePath "…" -ArgumentList "/quiet" -Wait
```

Three workflows currently require Windows: `migrate-database.yml`, `analyze-all-versions.yml`
(`runs-on: windows-latest # Required for Access ODBC driver support`), and two `ci.yml` jobs.
Trading a vendor-hosted MS installer for `apt-get install mdbtools` is a clear net reduction in
supply-chain fragility — that URL is a single point of failure entirely outside our control.

### 6. Could a SQLAlchemy dialect sit on top?

**No existing dialect.** `sqlalchemy-access` is the only Access dialect, and **[DOC]** its PyPI
metadata for 2.0.3 is decisive: `Operating System :: Microsoft :: Windows`, and
`requires_dist: ['SQLAlchemy>=2.0.0', 'pyodbc>=4.0.27', 'pywin32']`. **[EMPIRICAL]** it simply
cannot be installed on Linux:

```
$ uv pip install "sqlalchemy-access>=2.0.3"
  … because sqlalchemy-access>=2.0.3 depends on pywin32 …
  we can conclude that your requirements are unsatisfiable.
hint: Wheels are available for `pywin32` on the following platforms:
      `win32`, `win_amd64`, `win_arm64`
```

That `pywin32` dependency — not pyodbc, not ODBC — is the actual root of our Windows lock-in.

**I tested the one route that could have made a dialect viable**, since assessing (b) fairly
required it. I built mdbtools 1.0.1 **with** its ODBC driver (needed flex/bison, which I
obtained by `apt-get download` + `dpkg-deb -x` into `/tmp` — no sudo), registered it via a
local `ODBCSYSINI`, and drove it with pyodbc 5.3.0:

```
CONNECT OK via mdbtools ODBC 1.0.1
--- tables() ---            88 tables, e.g. ['MSysObjects', 'MSysACEs', …]
--- primaryKeys('Release') --- FAILED: [IM001] Driver does not support this function (SQLPrimaryKeys)
--- foreignKeys('Cell') ---    FAILED: [IM001] Driver does not support this function (SQLForeignKeys)
--- SELECT ---
   (1, '3.4', datetime.datetime(2024, 2, 6, 0, 0), False, '083F7C98-D0CF-4D87-A2E1-9DC464712242')
```

Two things stand out. The **data path looks attractive at first** — native `datetime`, native
`bool`, and **correctly formatted GUIDs** (because `odbc.c:112` calls
`mdb_set_repid_fmt(MDB_NOBRACES_4_2_2_2_6)`, which the CLI never does). *(But see
[Remediation](#remediation-for-the-date-bug): the ODBC data path shares the `data.c:897` date
bug and raises `ValueError` on affected rows, so the tempting "ODBC for data, mdb-schema for
metadata" hybrid does **not** work.)* And the **metadata path is a dead end. [SOURCE]**
`src/odbc/odbc.c:255-322` — both functions are empty stubs:

```c
SQLRETURN SQL_API SQLForeignKeys( … ) { TRACE("SQLForeignKeys");  return SQL_SUCCESS; }
SQLRETURN SQL_API SQLPrimaryKeys( … ) { TRACE("SQLPrimaryKeys");  return SQL_SUCCESS; }
```

They return success with no result set. `SQLColumns` *is* genuinely implemented (`odbc.c:1077`,
builds a full 18-column result set) but its output is not good enough:

```
--- columns('Release') ---
  ReleaseID     type_name=LONGINT   size=None  nullable=0
  Code          type_name=TEXT      size=None  nullable=0
  Description   type_name=TEXT      size=None  nullable=0   <-- actually nullable
  IsCurrent     type_name=BOOL      data_type=65529         <-- not a valid ODBC type code
  RowGUID       type_name=REPID     data_type=65535         <-- ditto
```

`column_size` is `None` for every column, `nullable` is `0` (`SQL_NO_NULLS`) for every column
including genuinely nullable ones, and `data_type` returns out-of-range codes for BOOL/REPID
that a dialect could not map. **A dialect over this driver would see no PKs, no FKs, no
lengths, and wrong nullability — strictly less than `mdb-schema` gives us for free.** Option
(b) is not merely expensive; it is worse than the cheap alternative. Ruled out.

#### Architecture comparison

| | Approach | Verdict |
|---|---|---|
| **(a)** | mdbtools → SQLAlchemy `MetaData` → SQLite → reflect the SQLite | **Adopt.** Validated end-to-end below. Reflection already lives here. |
| **(b)** | SQLAlchemy dialect over mdbtools | **Reject.** ODBC catalog functions are stubs; CLI is strictly richer. |
| **(c)** | pyodbc on Windows + mdbtools fallback (dpmcore's model) | **Transitional only.** Two fidelity profiles to maintain forever. |
| **(d)** | Pure-Python Access reader (`access-parser`, `jackcess` via JVM) | **Not evaluated.** See open questions. |

#### (a) validated end-to-end — this is the pivotal test

I built the full thing in `/tmp/mdbtools-research/build_from_mdbtools.py`:
`mdb-schema … access` for types and case → PKs from the sqlite backend → FKs from
`MSysRelationships` → **our own unmodified `add_foreign_keys_to_table` and
`heal_cross_table_foreign_keys`** → `MetaData.create_all` → load `mdb-export -0` CSV.

```
tables=63 cols=370 tables_with_pk=63
FKs from MSysRelationships: 156
FKs total after transformations.py: 181
SQLite schema created OK
loaded rows total=2697114 across 63 tables
out-of-range dates repaired: 11 ['ModuleVersion.FromReferenceDate', 'OperationScope.FromSubmissionDate']
```

Then the **real CLI generator** (`sqlite_to_schema` → `schema_to_sqlalchemy`, the path
`cli.py:279-282` actually uses):

```
mdbtools-derived(3.5): 51291 chars, 63 classes, 181 FKs, 181 rels -> compiles OK
groundtruth(3.2):      50727 chars, 63 classes, 180 FKs, 180 rels -> compiles OK
```

And the generated ORM, imported and queried against the mdbtools-built database:

```
Release rows: 2
  id=1 code='3.4' date=datetime.date(2024, 2, 6) cur=False guid=UUID('083f7c98-d0cf-4d87-a2e1-9dc464712242')
  id=2 code='3.5' date=datetime.date(2024, 7, 11) cur=False guid=UUID('dba8f305-b0af-0944-a5f3-4c569b8afdb2')
Concept rows: 599154
OperationVersion vid=1 release=3.4 expr= with {tC_01.00, c0010, default: 0, interval: true}: {r0
Concept 000043e1-d9a2-4ee4-a00c-62a811823ce1 -> DPMClass.name= Context | Organisation= European Banking Authority
total rows in DB: 2697114
```

Proper `datetime.date`, proper `bool`, proper `uuid.UUID`, and **multi-hop relationship
traversal working**. The generated `Concept` even emits the correct
`ForeignKey("DPMClass.ClassID")`, matching the committed `dpm2/models.py`.

**One required transformation surfaced:** mdbtools reports all 7 date columns as Access
`DateTime`, but `type_registry.column_type` narrows all of them to `Date` (only
`StartDate`/`EndDate` map to `DateTime`, and this DB has neither). The loader must therefore
store date-only values, or SQLAlchemy raises
`ValueError: Invalid isoformat string: '2024-02-06 00:00:00.000000'`. Straightforward, but it
must be done deliberately.

---

## What we would lose

Blunt list. Most of it we already throw away.

| Lost | Real cost |
|---|---|
| Access `NOT NULL` as authoritative | **None.** We overwrite it with data-derived nullability (`processing.py:84-85`). |
| Index definitions | **None.** `processing.py:74` clears them. |
| Access-specific SQLAlchemy types | **None.** `genericize` calls `as_generic()` anyway. |
| VARCHAR lengths *if* you use the sqlite backend | **Avoidable** — use `access`/`postgres` backend. |
| Precision/scale on `NUMERIC`, exact `CURRENCY` | **Zero today** (DPM has none), **but a latent trap** if EBA adds a decimal column. Add a guard that fails the build on an unmapped Access type. |
| 11 non-enforced FKs from `mdb-schema` | **None** — readable from `MSysRelationships`. |
| Correct GUID string form | **None** — repairable to exactly our current representation. |
| **Dates > ~2700 AD** | **Real.** Silent corruption of DPM's `9999-12-31` sentinel. Must be explicitly worked around. |
| pyodbc's native Python typing | Minor. CSV means we re-derive types — but that is what `type_registry` already does for SQLite. |
| Queries/views (`MSysQueries`) | Not currently used. |

We would also **gain**: the 11 non-enforced relationships as visible metadata, `DEFAULT`
values, `SERIAL`/autonumber detection, and native Access type names — none of which survive
`as_generic()` today.

## What would break in our pipeline

Specific files and functions in `src/dpm_toolkit/`.

**Must change:**

- **`migrate/processing.py`**
  - `access()` (line 29-33) — builds an `access+pyodbc://` engine. Needs a sibling
    `access_mdbtools()` or replacement returning `MetaData` + row iterators rather than an
    `Engine`.
  - `reflect_schema()` (41-49) and `genericize()` (36-38) — both are `Engine`-based
    reflection hooks. With mdbtools there is no Access `Engine`, so schema construction must
    be built from `mdb-schema` output instead. `genericize` becomes unnecessary (we choose
    generic types directly).
  - `schema_and_data()` (52-94) — currently interleaves `select(table)` per table over a live
    connection. Must become "read CSV per table". The *rest* of its body
    (`parse_rows`, `indexes.clear()`, `sqlite_with_rowid`, `CheckConstraint`, the nullability
    loop, `add_foreign_keys_to_table`, `heal_cross_table_foreign_keys`) is **source-agnostic
    and needs no change** — this is most of the function.

- **`migrate/transformations.py`** — `parse_rows()` (17-45) assumes `Row` objects with
  `_asdict()` and real Python types. With CSV it receives strings, so it needs a typed-coercion
  step in front (NULL sentinel → `None`, `0/1` → `bool`, ISO → `date`/`datetime`, GUID repair,
  **`1900-01-00` → `9999-12-31`**). `add_foreign_keys_to_table`,
  `heal_cross_table_foreign_keys`, `_pick_canonical_owner`, `_resolve_pk_targets` are all pure
  `MetaData` operations — **unchanged**.

- **`analysis/main.py`** — `create_engine_for_database()` (lines 57-91) has its **own,
  independent Access path**: it accepts `.mdb`/`.accdb` and builds
  `access+pyodbc:///?odbc_connect=…` (lines 81-88). This is a second Windows-only entry point,
  separate from `migrate/`, and it is why `analyze-all-versions.yml:68` pins
  `windows-latest # Required for Access ODBC driver support`. Migrating `migrate/` alone would
  **not** free that workflow — this function needs the same treatment (or `analyze` should be
  restricted to SQLite, since the pipeline can now produce one on Linux).

- **`pyproject.toml:20`** — the `sqlalchemy-access>=2.0.3; sys_platform == 'win32'` marker,
  plus a documented mdbtools ≥ 1.0.0 runtime requirement and a version check.

- **CI:** `migrate-database.yml` (drop `windows-latest` and the whole Access Database Engine
  download/verify/install block), `analyze-all-versions.yml:68`, and the two `windows-latest`
  jobs in `ci.yml`.

**Needs no change — verified by running it:**

- **`schema/main.py`** — `sqlite_to_schema`, `reflect_tables`, `detect_types`,
  `read_only_sqlite`. Reflects SQLite; indifferent to provenance.
- **`schema/type_registry.py`**, **`schema/enum_detection.py`**,
  **`schema/type_conversion.py`**, **`schema/sqlalchemy_export.py`** — all downstream of
  SQLite.
- **`analysis/inference.py`** and **`analysis/statistics.py`** — engine-agnostic; they consume
  whatever `create_engine_for_database` returns, so they need no change (only the engine
  factory above does). Two clarifications worth making explicitly, because the framing of the
  original question suggested otherwise:
  - They are **not** the mechanism that closes the CSV fidelity gap. That is
    `schema/type_registry.py`'s name-based rules, applied during reflection in
    `schema/main.py:detect_types`. `analysis/inference.py` is a separate *advisory* tool that
    emits `TypeRecommendation`s with confidence scores; nothing in the `convert` → `schema`
    path consumes its output.
  - So the hypothesis "does `analysis/inference.py` + `schema/type_conversion.py` close the
    gap?" resolves as: **the gap is closed, but by `type_registry.py`, not by `analysis/`.**
    That is a better outcome — the recovery is deterministic name matching (100% agreement,
    Finding 3) rather than confidence-scored statistical inference.
- **`projects/dpm2/src/dpm2/models.py`** — remains producible; generated equivalents compile
  and query correctly.

**Note:** `schema/generation.py` (the `Model` class) appears to be **legacy/dead code** — the
CLI uses `schema_to_sqlalchemy` from `sqlalchemy_export.py` (`cli.py:258-282`). The two
disagree: `generation.py:227` emits `ForeignKey("DPMClass.class_id")` (snake_case, which fails
to resolve — `NoReferencedColumnError`), while `sqlalchemy_export.py` and the committed
`models.py` both use `ForeignKey("DPMClass.ClassID")`. This is **pre-existing and unrelated to
mdbtools** — it reproduces identically from ground truth — but it is worth deleting or fixing.

---

## How dpmcore gets away with it (and why that doesn't transfer)

**Verified, and the asymmetry is exactly as suspected.** Their own module docstring
(`loaders/migration.py:1-16`) says they load into "any SQLAlchemy-supported database,
**preserving the ORM schema created by `Base.metadata.create_all`**". Their ORM is
hand-written across `orm/glossary.py`, `orm/infrastructure.py`, `orm/operations.py`,
`orm/rendering.py` (~4,700 lines).

**They never reflect anything.** `grep -n "reflect\|MetaData(\|autoload"` over
`loaders/migration.py` and all of `orm/` returns **nothing**. mdbtools is a pure **data pump**
into a schema they already own. So metadata loss is free for them, and their approach does
**not** transfer to a project that generates typed models from introspection — *if* our
generator reflected Access. It doesn't, which is precisely why the approach transfers to us
anyway, for a completely different reason than it works for them.

They also tolerate losses we would not have to. `_extract_with_mdbtools`
(`migration.py:160-187`) calls:

```python
subprocess.check_output(["mdb-export", access_path, table], text=True)
df = pd.read_csv(StringIO(csv_text), dtype=str)
```

**No flags at all** — no `-0`, no `-T`, no `-B`. So in their main migration path NULL and
empty string are indistinguishable, and every date is ambiguous `MM/DD/YY`. Their separate
`export_csv.py:130-133` patches only one table:

```python
cmd = ["mdb-export", "-d", ","]
if table in _DATE_FORMAT_TABLES:      # _DATE_FORMAT_TABLES = ("Release",)
    cmd += ["-T", "%Y-%m-%d"]
```

That `_DATE_FORMAT_TABLES = ("Release",)` is exactly the fingerprint of hitting the
`MM/DD/YY` default and fixing the one table where it was noticed. They also skip all `MSys`
tables (`_SYSTEM_TABLE_PREFIXES`), discarding the relationship metadata that turned out to be
our cleanest FK source.

**We should not copy their invocation.** Their flag usage is the weakest part of their
approach, and using `-0`/`-T` costs nothing.

---

## Open questions and what I could not determine

Stated plainly rather than papered over.

1. **No same-release baseline.** The `.accdb` is 3.5; the reference SQLites are 3.2/4.1, and
   `3.2-sample-original.sqlite` is a **sample** (15,350 Concepts vs 599,154). Producing a
   pyodbc-derived 3.5 SQLite requires Windows. **Consequence:** the residual nullability
   differences in the generated models (which flip in *both* directions) are *consistent with*
   a data/release difference but **not proven** to be one. This is the single most valuable
   follow-up: run the current Windows pipeline on this exact 3.5 file and diff.

2. **Is `IF_TM.FromReferenceDate` really `9999-12-31` in 3.5?** I proved mdbtools *would*
   corrupt such a value (`data.c:897` guard + zeroed `struct tm`), and ground truth 3.2 has
   `9999-12-31` at that row. But I could not read the 3.5 file with a second tool to confirm
   the 3.5 value specifically — and I have now established that **no mdbtools code path can
   read it** (see [Remediation](#remediation-for-the-date-bug)), so confirming this requires a
   non-mdbtools reader. The bug is certain; its exact blast radius on *this* file (11 rows)
   assumes those 11 are all far-future sentinels rather than pre-1900 dates.

3. **Encrypted/password-protected/compacted `.accdb` untested.** DPM files are none of these
   today. If EBA ever ships one, this needs re-testing.

4. **`mdb-schema --relations` for the `access`/`sybase` backends** is silently a no-op (no
   `MDB_SHEXP_RELATIONS` capability). I used postgres/MSysRelationships instead and did not
   chase whether that is considered a bug upstream.

5. **Option (d) not evaluated.** Pure-Python readers (`access-parser`) and `jackcess` (JVM)
   were out of scope. `access-parser` in particular could remove the subprocess dependency
   entirely and deserves its own look — I have no evidence about its `.accdb` fidelity.

6. **`analysis/` not exercised.** I did not run `analysis/main.py` over the mdbtools-derived
   database, so I cannot claim its reports match. Its SQLite path should work unchanged, but
   that is inference, not measurement. Separately, its `.accdb` branch
   (`analysis/main.py:81-88`) is a second Windows-only dependency I found late and did not
   design a replacement for.

7. **Enum comparison was impossible.** `3.2-sample-original.sqlite` has **0 CHECK
   constraints**, so it predates the enum feature; the committed `models.py` has 21
   `Literal[...]`. I verified the enum *inputs* survive (22 candidate columns with recoverable
   distinct values) but could not diff generated enums against a ground truth.

8. **Currency / decimal / binary fidelity is untestable on this database.** DPM contains no
   `Currency`, `Numeric`, `Byte`, `Single`, `Binary`, or `OLE` columns, so the worst
   sqlite-backend type defects (`NUMERIC → INTEGER` destroying precision/scale,
   `MONEY → REAL`) are **unexercised, not disproven**. If EBA ever adds a decimal or currency
   column this needs re-testing. Mitigation is cheap and I'd do it regardless: make the DDL
   parser raise on any Access type it does not explicitly map, so a new type fails the build
   rather than silently degrading. My prototype already does this
   (`UNMAPPED TYPE … on <table>.<column>`).

9. **Whether a *non-mdbtools* reader can recover the poisoned dates.** Resolved for mdbtools
   (no path can — see Remediation), but I did not test `access-parser` or a Windows pyodbc read
   against this 3.5 file, which is what would actually recover the 11 true values.

10. ~~**Does the mdbtools ODBC driver share the date bug?**~~ **Resolved — yes.** Kept here to
    record that it was an open question; the evidence and consequences are in
    [Remediation](#remediation-for-the-date-bug).

---

## Corrections

Recorded because two of these are traps an implementer will otherwise hit, and one is a
correction to the framing of the original brief.

### Corrections to my own earlier findings in this investigation

1. **The `split(';')` DDL-loading trap — this one will bite whoever implements this.** I first
   reported that 2 `CREATE INDEX` statements failed with `no such table: main.Operation` and
   that only 49 of 63 tables loaded, losing 14 primary keys. **Both were artifacts of my own
   loader, not mdbtools.** I was splitting the DDL on `;` and keeping fragments that
   `startswith("CREATE")`. `mdb-schema` interleaves `-- CREATE INDEXES ...` comment lines
   between statements, so after splitting, a fragment looks like
   `"-- CREATE INDEXES ...\nCREATE INDEX ..."` — which fails the `startswith` test and gets
   dropped silently, taking a real statement with it. Using `sqlite3.executescript(ddl)` on
   the whole file loads everything: **63 tables, 71 indexes, 63/63 PKs, zero errors.** Do not
   hand-roll a statement splitter over `mdb-schema` output.

2. **`schema/generation.py` is not the generator in use.** I first ran the end-to-end model
   test through `generation.py`'s `Model` class and got
   `NoReferencedColumnError: Could not initialize target column for ForeignKey
   'DPMClass.class_id'`. That is **not an mdbtools problem** — it reproduces identically from
   ground truth. `generation.py:227` emits `ForeignKey("DPMClass.class_id")` (snake_cased,
   unresolvable), whereas the CLI actually uses `schema_to_sqlalchemy` from
   `sqlalchemy_export.py` (`cli.py:258-282`), which emits `ForeignKey("DPMClass.ClassID")` and
   matches the committed `dpm2/models.py`. `generation.py` looks like dead code and should be
   deleted or fixed. **All end-to-end results in this report use the real
   `schema_to_sqlalchemy` path.**

3. **`analysis/` is not purely a SQLite consumer.** I initially wrote that
   `analysis/inference.py` and `statistics.py` "operate on a SQLite database, so they are
   unaffected." Incomplete: `analysis/main.py:57-91` (`create_engine_for_database`) accepts
   `.mdb`/`.accdb` and builds `access+pyodbc:///?odbc_connect=…` at lines 81-88. That is a
   **second, independent Windows-only entry point**, and it is why
   `analyze-all-versions.yml:68` pins `windows-latest`. Migrating `migrate/` alone would not
   free that workflow. Corrected in
   [What would break](#what-would-break-in-our-pipeline).

4. **The GUID byte-order check was initially inconclusive and I nearly over-read it.** The raw
   intersection with ground truth was only 3 of 15,350, which looks like failure. It is not —
   it is the sample/release difference — but 3 exact 32-hex-character collisions cannot be
   chance, which is what makes the check valid. I only trusted it after testing all endianness
   variants (raw 3, byte-swapped 0, reverse-swapped 0, full-reverse 0).

### Corrections to the framing of the original question

5. **"Reflection moves downstream, off Access entirely" is not a change to make — it is
   already the architecture.** The brief presented this as the distinguishing property of
   option (a). `schema/main.py:106` (`sqlite_to_schema`) reflects the **SQLite**, and the
   `schema` CLI command takes a `.sqlite` path. Access reflection exists only inside
   `migrate/`. This is the single fact that makes the answer "yes" rather than "only as a
   fallback".

6. **The pivotal hypothesis is right about the outcome but wrong about the mechanism.** The
   brief asked whether `analysis/inference.py` + `schema/type_conversion.py` close the CSV
   fidelity gap. The gap *is* closed — 100% type agreement — but by
   **`schema/type_registry.py`**, whose deterministic name rules run inside
   `schema/main.py:detect_types` during reflection. `analysis/inference.py` is an *advisory*
   tool that emits confidence-scored `TypeRecommendation`s, and **nothing in the
   `convert` → `schema` path consumes its output**. This is a better result than the
   hypothesis: the recovery is exact name matching, not statistical inference with thresholds.

7. **Unverified premise, flagged not answered.** The brief states the Windows-runner
   requirement "is the reason the `dpm2` package ships a 88 MB prebuilt SQLite DB." I found no
   evidence for or against that causal link and did not investigate it.

8. Minor: the brief locates `_DATE_FORMAT_TABLES` at `export_csv.py` line ~20 — correct
   (defined line 20, used lines 131-132).

---

## Remediation for the date bug

This is the one defect standing between the verdict and a clean implementation, so it gets its
own section. **Short version: no mdbtools reader can recover the true value, so the fix is a
loud detection gate plus an out-of-band verification — not a smarter parse.**

### What I established empirically (all new, all on this machine)

**(i) No format string sidesteps the guard.** The obvious hope is that `-D`/`-T` only affect
rendering and some format would expose the real value. It does not — every field of the
`struct tm` is zero:

```
$ for f in '%Y' '%Y-%m-%d' '%s' '%j day-of-year' '%Y|%m|%d|%H|%M|%S'; do
      mdb-export -0 '\N' -T "$f" "$DB" ModuleVersion | awk -F, '$1==353{print $10}'; done
  -T '%Y'                   -> "1900"
  -T '%Y-%m-%d'             -> "1900-01-00"
  -T '%s'                   -> "-2209078814"
  -T '%j day-of-year'       -> "001 day-of-year"
  -T '%Y|%m|%d|%H|%M|%S'    -> "1900|01|00|00|00|00"
  (contrast, a real row) ModuleVID 337 -> "2023"
```

`%s` yielding `-2209078814` is the epoch value *of* 1900-01-00, not of the true date. The
double is discarded before formatting, so **the information is gone by the time any format
string is applied.**

**(ii) `mdb-json` has the same bug.** Not a separate code path worth trying:

```
$ mdb-json "$DB" ModuleVersion | grep IF_TM
{"ModuleVID":353,…,"FromReferenceDate":"01/00/00 00:00:00",…}
```

**(iii) The ODBC driver has the same bug, and fails *harder*.** This kills the hybrid idea
before it starts. **[SOURCE]** `src/odbc/odbc.c:1448` calls **the same function**:

```c
case MDB_DATETIME:
    mdb_date_to_tm(mdb_get_double(mdb->pg_buf, col->cur_value_start), &tmp_t);
```

**[EMPIRICAL]** via pyodbc + the locally-built mdbtools ODBC driver:

```
$ SELECT ModuleVID, Code, FromReferenceDate, ToReferenceDate FROM ModuleVersion
ValueError: day 0 must be in range 1..31 for month 1 in year 1900

$ SELECT … WHERE ModuleVID < 350     ->  11 rows, OK
$ SELECT … WHERE ModuleVID = 353     ->  ValueError (same)
```

So the ODBC path does not merely corrupt the value — it makes the **whole fetch raise**,
because pyodbc refuses to construct a `datetime.date` from day 0. Unpoisoned rows fetch fine.

**Consequence for the hybrid architecture the brief asked about** ("mdb-schema for metadata,
ODBC for data rows only, since the ODBC stubs only hurt metadata"): the reasoning was sound,
but **it does not work.** The ODBC data path shares the identical root cause, and its failure
mode is worse than the CLI's. It also buys less than it appears to: its one genuine advantage
over `mdb-export` is correctly-formatted GUIDs (`odbc.c:112` sets
`MDB_NOBRACES_4_2_2_2_6`), and that problem is already solved for free by stripping non-hex
characters. **Recommendation: do not build the hybrid.** Use `mdb-export` for data.

### The constraint this creates

`1900-01-00` is produced by **both** branches of the guard — `td > 1e6` (after ~2700 AD) *and*
`td < 0.0` (before 1899-12-30). So even after detecting it, the output is ambiguous between
"far future" and "pre-1900". Nothing in the CSV disambiguates.

The one thing in our favour: **`1900-01-00` is an impossible date** (day 00), so detection is
*exact* and can never false-positive on real data. That makes a gate reliable even though a
repair is a guess.

### Options weighed

| Option | Recovers true value? | Verdict |
|---|---|---|
| Different `-D`/`-T` format | No — (i) | Dead |
| `mdb-json` / `mdb-array` | No — (ii) | Dead |
| mdbtools ODBC driver for data rows | No, and raises — (iii) | Dead |
| Detect `1900-01-00` post-export, map to `9999-12-31` | No (assumes far-future) | **Ship this**, with a gate |
| Map to `NULL` instead | No | **Wrong** — Access declares `FromReferenceDate` `NOT NULL`; this is what produced `IntegrityError: NOT NULL constraint failed` in my run |
| Patch `data.c:897` and ship our own build | **Yes** | Correct but costly — forfeits distro packaging, which is the main reason to adopt mdbtools |
| Verify out-of-band (Windows pyodbc / `access-parser`) once per release | **Yes**, for the affected rows | **Do this as the companion step** |

### Recommended fix

**Pick: detect-and-gate with an explicit allowlist, mapped to `9999-12-31`, plus a one-off
out-of-band verification. Do not patch mdbtools; do not build the ODBC hybrid.**

Rationale: patching recovers the true value but forfeits `apt-get install mdbtools`, which is
most of the benefit of the whole migration — and the guard is present in every version we can
realistically install (Ubuntu 24.04 → 1.0.0) and in current upstream `HEAD`, so we would be
carrying a patched build indefinitely. The gate costs almost nothing and converts a silent
corruption into a build failure.

Concretely, in the CSV coercion step in `migrate/transformations.py`:

1. **Treat `1900-01-00` as a poison value, never as a datum.** Match on the impossible day-00
   form, not on `1900` (real 1900 dates would be legitimate).
2. **Map to `datetime(9999, 12, 31)`** — DPM's open-ended sentinel, and what ground truth 3.2
   holds at the one row I could cross-check. Never map to `NULL`.
3. **Gate on an explicit allowlist** of `(table, column)` → expected row count. Today:
   `ModuleVersion.FromReferenceDate` and `OperationScope.FromSubmissionDate`, **11 rows
   total**. Fail the conversion if the observed set is not a subset of the allowlist, or if a
   count exceeds its expected value.
4. **Assert the total independently.** Also fail if the *global* count of poisoned values
   changes, so a future release that introduces far-future dates in a column nobody is watching
   trips the build rather than silently acquiring a wrong date. This is the answer to "how do I
   make it fail loudly for columns I'm not watching": don't enumerate columns to watch — watch
   *all* columns for the impossible value and allowlist the known-good exceptions. The check is
   cheap because the sentinel is exact.
5. **Log every repair** with table, column, and row key, so the substitution is auditable
   rather than invisible.

**Companion step, once:** confirm the 11 true values with a non-mdbtools reader — a single
Windows `pyodbc` run over this same 3.5 file, or `access-parser`. That retires the
far-future-vs-pre-1900 assumption in step 2. Until then it is an assumption, and the report
should be read as such. It is a well-founded assumption for regulatory reference dates, but it
is not a measurement.

**Also worth doing:** file an upstream issue. The bound looks like a rough guess rather than a
computed limit — the comment reads `// About 2700 AD`, while Access's true maximum
(`9999-12-31`) is 2,958,465 days. The surrounding arithmetic already handles large years
(`tm_year = yr - 1900` → 8099, which `strftime %Y` renders correctly), so raising the bound
appears to be a one-line fix. Worth contributing, but **do not gate adoption on it** — we
cannot rely on distro packages carrying it for years.

### Additional failed experiments (not already recorded)

- **`mdb-schema -I sqlite` (0.7.x flag form) rejected by 1.0.1:**
  `option parsing failed: Unrecognized option: -I`. In 1.0.x the backend is a **positional**
  argument. My first six backend dumps produced 0 lines each before I noticed.
- **`-b hex` silently corrupted every GUID column.** I passed it defensively on the first full
  export, then found `RowGUID` values like `7B30383346374339382D…`. Decoding proves it
  hex-encoded the already-formatted GUID *string*:
  `bytes.fromhex('7B3038…7D').decode()` → `{083F7C98-D0CF-4D87-A2E19DC464712242}`. I had to
  re-export all 63 tables without it. DPM has no binary columns, so `-b` should never be passed
  here.
- **First full data load raised** `sqlalchemy.exc.StatementError: SQLite DateTime type only
  accepts Python datetime and date objects` — my coercion gap (CSV gives strings), not a
  mdbtools issue.
- **Second full load raised** `IntegrityError: NOT NULL constraint failed:
  ModuleVersion.FromReferenceDate` — this *was* the date bug, and is the evidence that mapping
  poisoned dates to `NULL` is not a viable strategy.
- **Generated-model smoke test raised** `InvalidRequestError: Table 'Concept' is already
  defined for this MetaData instance` — caused by importing the generated module alongside the
  real `dpm2` package, which registers the same tables. Fixed with a stub `dpm2.base` providing
  only `DPM`. A harness artifact, but worth knowing if anyone re-runs this comparison.
- **A multi-case `cur.columns()` loop over a single ODBC connection produced no output at all**
  — almost certainly a segfault in the driver. One fresh cursor per catalog call works. The
  mdbtools ODBC driver is fragile enough that this is worth noting independently of the stub
  issue.

---

## Reproduction

```bash
# 1.0.1 built locally, no sudo:
cd /tmp/mdbtools-research
curl -sLO https://github.com/mdbtools/mdbtools/releases/download/v1.0.1/mdbtools-1.0.1.tar.gz
tar xzf mdbtools-1.0.1.tar.gz && cd mdbtools-1.0.1
./configure --prefix=/tmp/mdbtools-research/local --disable-glib && make -j4 && make install

export M=/tmp/mdbtools-research/local/bin
export LD_LIBRARY_PATH=/tmp/mdbtools-research/local/lib
DB="/home/jim/dpm/assets/DPM 2.0_withOperations_3.5.accdb"

$M/mdb-ver "$DB"                                     # ACE14
$M/mdb-schema --indexes --not-null "$DB" access      # native types, original case
$M/mdb-schema --relations "$DB" postgres             # 148 FKs
$M/mdb-export -H -q '' "$DB" MSysRelationships       # all 159 relationships incl. grbit=2
$M/mdb-export -0 '\N' -T '%Y-%m-%d %H:%M:%S' "$DB" Release   # do NOT add -b hex
```

Full (a) prototype: `/tmp/mdbtools-research/build_from_mdbtools.py`.
Generated models: `real_mdb.py` (mdbtools 3.5) and `real_gt.py` (ground truth 3.2).

---

## Live validation against DPM 4.3.1 and 4.4-draft (2026-09-10)

The report above was written against `DPM 2.0_withOperations_3.5.accdb` with
`3.2-sample-original.sqlite` as reference — different releases, and the
reference a *sample*. That gap is now closed. Both variants of two current
releases were fetched with `dpm-toolkit download` (`--variant original` for the
`.accdb`, `--variant converted` for the pyodbc-derived SQLite), giving a
same-release baseline on both sides.

**4.4-draft is the authoritative comparison.** Its `converted` artifact was
published 2026-09-05, after `add_self_referential_foreign_keys` landed
(2026-09-01), so it reflects current pipeline behaviour. The 4.3.1 artifact
predates that commit, which is why its FK count is 206 where mdbtools produces
217 — a stale artifact, not a reader difference.

### Structure: exact

| | 4.4-draft reference | mdbtools |
|---|---|---|
| tables | 70 | 70 |
| columns | 477 | 477 |
| NOT NULL differences | — | 0 |
| primary key differences | — | 0 |
| foreign keys | 217 | 217 (0 missing, 0 extra) |
| row-count differences | — | 0 |
| rows | 5,807,457 | 5,807,457 |

The only declared-type difference is 51 replication-ID columns: the reference
declares them `INTEGER`, which is wrong for a hex string; mdbtools yields
`CHAR(36)`.

### Data: 62 of 70 tables byte-identical

All 5,807,457 rows were compared on full column content, order-insensitively.
Every differing cell is accounted for:

| Cause | Cells | Reducible? |
|---|---|---|
| Zero-length string read as NULL | 2,962 | **No** — see below |
| `VariableGeneration.StartDate`/`EndDate` off by one second | 344 | No — mdbtools rounds, pyodbc truncates; the fraction is gone before output |
| `Item.Name` non-BMP characters replaced by `?` | 1 | No — see below |

That is 3,307 cells of roughly 40 million.

**Zero-length strings.** mdbtools reports Access's zero-length strings as NULL.
Where Access declares the column NOT NULL the ambiguity resolves and the empty
string is restored (`ItemCategory.Signature`, 8 rows in 4.3.1 — this is why the
NOT NULL differences are 0). Where the column is nullable it cannot be
resolved, because the reference holds *both* spellings in the same column:

```
table.column                             empty    null    total
Item.Description                          1865   12439    15815
OperationVersion.Description              1033    2283    19206
TableVersion.Description                    27     237     2687
SubCategory.Description                     10     989     1206
TableAssociation.Description                15      25       42
Category.Description                         4      58      155
ModelViolations.HeaderCode/Direction         8       —       44
```

Both spellings mean "no description", but `WHERE Description IS NULL` returns
1,865 more `Item` rows after conversion. **This is a semantic change to a
published artifact and needs an explicit decision.**

**Non-BMP characters.** `Item.Name` 1012406831 is `𝑚 𝐶𝑉𝐴 multiplier factor`
using mathematical-italic characters outside the BMP. Jet/ACE stores text as
UCS-2, and mdbtools substitutes `?` per surrogate half, giving
`?? ?????? multiplier factor`. Not a locale problem — reproduced identically
under `LC_ALL=C.UTF-8` and `en_US.UTF-8`. This is silent corruption and is not
reliably detectable, since `?` is a legitimate character.

### Bugs this found in the implementation

Four, none of which the 3.5 file exposed:

1. **`Numeric` was unmapped.** 4.3/4.4 contain `Numeric (19, 0)` columns
   (`ChangeLog.Timestamp`, `OperationNode.AbsoluteTolerance`/
   `RelativeTolerance`). Worse, the two-value size specifier did not match the
   column regex at all, so the line was *skipped silently* rather than caught
   by the unmapped-type guard. Both fixed; a column line that fails to parse
   now raises.
2. **Access internal tables leaked through.** `~TMPCLP531411` and
   `~TMPCLP571251` — clipboard scratch tables — became real tables. Now
   filtered along with `MSys*`.
3. **GUIDs were written in the wrong format.** The 3.2 sample stores bare
   lowercase hex, so the original reader normalised to that. Current artifacts
   store canonical uppercase hyphenated (`083F7C98-D0CF-4D87-A2E1-9DC464712242`).
   Fixed, which took 4.4 from 4/13 to 58/70 tables identical.
4. **Newlines inside memo fields were lost.** Two compounding causes:
   `subprocess.run(text=True)` applies universal-newline translation, rewriting
   CRLF to LF before parsing; and `splitlines()` splits *inside* quoted CSV
   fields, concatenating multi-line values. Fixed by decoding bytes manually
   and reading through `StringIO(..., newline="")`.

### Cost

4.3.1 and 4.4-draft are ~681 MB Access files. Conversion took 4–13 minutes
(varying with machine load) at a peak RSS of ~3.3 GB. Rows are materialised
eagerly, as the pyodbc path also does, so this is not a regression — but it is
worth knowing before running it on a constrained runner.
