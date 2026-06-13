---
name: adr
description: Create a new Architectural Decision Record under docs/adr/. Use when the user asks to "write an ADR", "record a decision", "add an ADR", or otherwise wants to capture a cross-cutting decision in the repo's decision log.
---

# adr

Create a new Architectural Decision Record in `docs/adr/`.

## What ADRs are for in this repo

`DESIGN.md` covers the overall architecture. ADRs capture the smaller,
sharper decisions that fall out of implementing it — choices that future
contributors need to understand in order to read the code as intentional
rather than accidental.

If the topic belongs in `DESIGN.md` (architectural shape, wire contract,
component responsibilities), update that instead. ADRs are for the decisions
that sit *below* the architecture.

## Procedure

1. **Find the next number.** List `docs/adr/` and pick the next sequential
   four-digit number after the highest existing `NNNN-*.md` file. ADRs are
   numbered from `0001`; do not reuse numbers, even for superseded ones.

2. **Pick a slug.** Short kebab-case, descriptive of the decision itself,
   not the problem it solves. Good: `single-go-module`,
   `component-naming-mirrors-contrib`. Bad: `module-question`,
   `what-to-name-things`.

3. **Copy the template.** Start from `docs/adr/template.md`. The shape is
   Status, Date, Context, Decision, Consequences. Do not invent new
   sections — if a decision needs more structure, it is probably more than
   one ADR.

4. **Fill it in.**
   - **Status:** `Proposed` until the user confirms acceptance, then
     `Accepted`. For decisions being made *as* the ADR is written (the
     common case during active development), `Accepted` with today's date
     is correct.
   - **Date:** today's date in `YYYY-MM-DD`.
   - **Context:** what prompted this decision. Name the alternatives that
     were on the table and why each is or is not viable. Avoid restating
     `DESIGN.md`; reference it by section number when needed.
   - **Decision:** the choice, as a positive imperative ("Use a single Go
     module at the repo root"), not a description ("We decided to...").
   - **Consequences:** what this makes easier, what it makes harder, and
     what it commits the project to revisit (and under what conditions).
     Be honest about the trade-offs; an ADR with only upside is suspect.

5. **Update the index.** Add a one-line entry to `docs/adr/README.md` under
   `## Index`, in numerical order:

   ```
   - [NNNN — Short title](NNNN-kebab-slug.md)
   ```

6. **Verify.** Confirm `make check` still passes (the ADRs are markdown so
   nothing should break, but the gate is cheap).

## Editing existing ADRs

ADRs are immutable once accepted. To change a decision:

1. Write a new ADR that supersedes the old one. Its `Context` section
   should briefly recap why the prior decision needs revisiting.
2. Edit the old ADR's `Status` line to read
   `Superseded by NNNN` where `NNNN` is the new ADR's number.
3. Do not delete or rewrite the old ADR's body.

Typo fixes and clarifications that do not change the decision are fine to
apply in place without superseding.

## Style notes

- Keep each section short. The whole ADR should fit on one screen if
  possible; if it does not, the decision is too broad.
- No marketing language. ADRs are written for the next contributor reading
  the code six months from now.
- Do not reference `DESIGN.md` from inside Go source or per-component
  READMEs — only from top-level docs and other ADRs. Code in this repo may
  be extracted into its own module; what ships with the code must stand
  alone.
