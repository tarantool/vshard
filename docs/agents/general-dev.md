# Development in VShard

This document explains how to write code in VShard, the expected standards,
approaches, style, formatting, and other rules.

## Commits

Patches for VShard are always organized as a sequence of atomic commits. That
is, such commits which are fully self-sufficient and make sense and bring value
on their own.

Each commit must be such that nothing can be removed from it without making it
lose its sense, and nothing needs to be added to make it valuable and working.

Each commit must pass luacheck and all tests.

The commit messages have a specific structure:
- Title has the format `system: imperative title`. For example, `storage: fix
  error logging in the recovery service`. The system is the affected module or
  area: `storage`, `router`, `replicaset`, `rebalancer`, `recovery`, `gc`,
  `cfg`, `util`, `error`, `test`, `ci`, `example`, `roles`, `doc`, and similar.
  The title is limited by 50 chars max, unless it is impossible for some reason.
- The message is descriptive, not narrating the code, explaining the reason for
  the commit, perhaps some general outline of what is done if the code is highly
  complex. The message lines are limited by 72 symbols in width unless it is
  impossible (for example, a long link is inserted).

The body of the message must reference a specific GitHub ticket when work is
related to one. For example, `Closes #661` or `Part of #582`.

Commit message must have in its end some tags:
- `NO_DOC=<reason>` - specified when the commit changes no documented behavior
  (no new public features or changes to existing ones), and thus has no
  `@TarantoolBot document` label in the message.
- `NO_TEST=<reason>` - when no tests are added or modified in the commit. It
  might normally happen for refactoring commits which change already covered
  code.

Unlike Tarantool's core, VShard does not use per-commit changelog files and has
no `NO_CHANGELOG` tag. The files in `changelogs/` are old and not used anymore.

Commit message might have to add a documentation request when a feature is
added or modified or deprecated. This is done in the form of:
```
@TarantoolBot document
Title: <title ...>
Since: VShard <version>
Multiline description ...
```
When the commit is merged into the main branch, there will be a GitHub ticket
automatically created with the given title and message. This text is encouraged
to use markdown formatting to make the ticket look nice. The `Since:` line
mentions the first VShard version which will have the feature. When no doc
request is added, the commit message must specify `NO_DOC` tag.

The ordering of commit message sections is: title -> message -> ticket ref ->
tags -> doc request.

Commit title, message, and the diff must exclusively use ASCII characters. The
only exception allowed can be something that explicitly requires a non-ASCII
character.

## Code

VShard's only language is Lua, running on LuaJIT inside Tarantool. There is no
compilation step - the module is executed as is.

VShard must keep working on a wide range of Tarantool versions, including very
old ones. Any usage of a Tarantool feature which is not present in all the
supported versions must be gated via `vshard.util.feature` flags or version
checks (`vshard.util.version_is_at_least`), and/or must have a wrapper making
this feature work on all the supported versions when possible.

VShard supports hot code reload - upgrading the module code on a running
instance without a restart. This imposes strict rules:
- All mutable module-level state must live in a table `M`, stored in `_G`
  under a `__module_vshard_<name>` key. On require, when the global is already
  present, the module must pick up the existing state instead of recreating it.
- Any change of the `M` table layout in `vshard.storage` must be reflected in
  the `vshard.storage.reload_evolution` module, which migrates the state of an
  older module version on reload. Other modules track a `MODULE_VERSION`
  constant which must be bumped when their behavior changes. It can also vary
  insignificantly between various modules how they handle the reload.
- Long-living background fibers must be created via `util.reloadable_fiber_new`,
  so a reload restarts their function from the new code.

Code must be modular. New functions must be added exactly to where they belong,
and must stay `local` to their file unless they absolutely need to be exposed.
Cross-module access without circular `require`s is done via `vshard.registry`.

Code must be performant on hot paths - request routing, bucket operations,
anything called per-request. There we avoid creating garbage (closures, tables)
and cache frequently used functions in file-local variables, like
`local fiber_clock = lfiber.clock`.

The performance value is superseded by the huge value of simplicity off the
hot paths, and only when chasing a small performance gain would require
disproportionately complex code. In any code that isn't hot we must at all
costs keep the code very simple and modular. Simple short functions, linear
code, good deep comments where necessary.

Existing code is encouraged to be changed when it helps to reuse it, or when it
can be even dropped as superseded by newer code. Legacy code, when possible,
must not be kept for any sort of "backward compatibility" or "for later
rework", unless it maintains some public behavior.

Comments in the code must never be narrative. If a comment just narrates the
next lines, then it must be deleted. If a comment somehow speaks about commits
and our current development plan, then it must be deleted. If a comment is just
obvious in any way, then it must be deleted. Do not write comments at all
unless the commented code is extremely complex and external context is needed
for understanding it. The exception is module and complex-function header
comments explaining the idea and reasons behind a subsystem, like the ones in
`vshard/storage/ref.lua` and `vshard/storage/sched.lua`.

## Style

- Text file line lengths (code, text, Markdown, etc) are limited by 80 chars.
- Indentation is 4 spaces, no tabs.
- Naming for variables, functions, tables is `like_this`.
- Single-line comments are `-- text`. Multi-line comment blocks start with a
  lone `--` line, continue with `-- text` lines, and end with a lone `--` line
  when used as a header of a function or a section.
- Luacheck must pass on all Lua files with the config `.luacheckrc` in the
  root. That includes the test files.

## Tests

Each changed line, unless it is a simple refactoring like a rename or a code
move, MUST be covered by tests.

Older tests might already exist sometimes, covering this place implicitly. Or
the refactoring might be such that some tests cover the changed code naturally
and explicitly.

But much more often than not, new tests should be written.

The preferable way of testing is Luatest, in the `test/*-luatest/` folders.
See `docs/agents/test.md` for the details of how the tests are organized and
written.

## Setup

Development in VShard is quite individual. You MUST load
`docs/agents/local_setup.md` to understand how to work in user's environment
for running tests and static analysis.

If and only if that file is missing or seems outdated, you must load
`docs/agents/setup-dev.md` to create/update the local setup file.

---

If anything in this document seems outdated from how the code actually works,
then it must be immediately flagged to the user.
