# Development setup

The document explains how to set up VShard's sources for development: the
environment, dependencies, running tests, static analysis.

VShard is pure Lua - there is nothing to compile. But the setup is still very
individual: where the Tarantool binary comes from, which version it is, how
luatest is installed, how the tests are usually run.

The user must be questioned about their local setup, their preferences, their
ways of development.

The questions need to enable you to do the things explained below, unless the
user explicitly opts out some of them and wants to do certain things manually.

## Environment

VShard requires a `tarantool` binary. For running tests it doesn't even need to
be in `PATH`.

Developers either install Tarantool from packages, or build the Tarantool
sources, optionally adding the build's `src/` and `extra/` directories to
`PATH`.

The Tarantool version matters - VShard supports old versions, and some tests and
features are gated by the core version. Find which binary is used.

The test infrastructure needs the `test-run/` git submodule
(`git submodule update --init --recursive`), `python3` with test-run's
requirements, and `luatest` for the luatest suites.

The way luatest is installed is individual (a `.rocks/` install in the repo, a
system rock, a checkout). You need to make sure that the tests run successfully,
as a sign that the installation of all the modules and dependencies worked out.

There is also a manually runnable example cluster in `example/`, managed by
its Makefile (`make start`, `make enter`, `make clean`, ...).

## Static analysis

All patches in VShard are subjects for static code analysis which is strictly
enforced in CI.

The local cheap must-have checks are `luacheck` and `checkpatch` tools.

Installation of these tools is highly individual. User needs to be asked about
that, and might even opt out and decide to do it manually.

Otherwise, when these tools are available (user will tell how to run them
then), they can be run as follows:
```Bash
luacheck --codes --config .luacheckrc .
perl -X -- checkpatch.pl --ignore NO_CHANGELOG --color=always --git <commit>
```

`Luacheck` must be used on all files having `.lua` extension. The tool via the
provided config will automatically decide which Lua files can be ignored, and
which will be validated. That includes all Lua files, including Luatest
`..._test.lua` files.

`Checkpatch` is a Perl tool from the `tarantool/checkpatch` repository which
validates the commit message and its diff. VShard does not use per-commit
changelogs, hence the `--ignore NO_CHANGELOG`.

## GitHub API for reviews

Publishing reviews on GitHub (`docs/agents/review-gh.md`) has an experimental
mode which works via the private API of the GitHub web UI. Enabling it requires
a browser cookie.

Normally one wouldn't want to extract and store a cookie of their main account
anywhere, as it might be dangerous. The user can be suggested to create a bot
account and use its cookie instead.

The experimental mode unlocks certain features unavailable in the public API.
See the above mentioned md file to find which features, and list them to the
user so they can make a decision.

Walk the user through this setup when they want the mode enabled:

1. Optional but very desirable. Create a separate GitHub bot account (it is
  allowed by the GitHub ToS) or use an existing one. Instead of using the main
  account.

If creating a new bot account and the user already has a gmail, there is a
convenient way to bind the bot account to the same email as the main account.

Simply use a gmail alias like `<your-original-gmail>+bot@gmail.com`. For
example, if the original email is `claude@gmail.com`, then a new GitHub account
for the bot can be bound to `claude+bot@gmail.com`. GitHub treats them as
separate emails, but in gmail they are treated as one email.

2. Create a personal access token for that account with only the `public_repo`
  scope. It covers all the public-API work of `tools/gh-review.py`, which must
  then always run with the token in the `GH_TOKEN` environment variable when the
  bot account should be used.

3. Create a bot's browser session cookie for the private API: the `user_session`
  and `__Host-user_session_same_site` values from a logged-in browser profile,
  formatted as one `Cookie` header value. It is passed to `tools/gh-review.py`
  in the `GH_COOKIE` environment variable as a `<login>:<cookie>` pair, where
  the login is the GitHub account the cookie belongs to - store it in exactly
  this form. If the session expires, just generate a new one.

The private API routes the posted comments by the cookie's session, and there
is no reliable way to check programmatically which account a cookie belongs to.
That is why the account login is kept inside the `GH_COOKIE` value itself. On a
cookie refresh, remind the user to extract it from the same account's browser
profile and to keep the login prefix correct. When the token and the cookie
resolve to different accounts, the review falls apart in a confusing way: the
comments land into an invisible pending review of the cookie's account, while
the token's review has nothing to submit.

Strongly suggest to the user to keep the token and the cookie in a secrets
storage, for example the macOS keychain, with a plain text file only as a
fallback.

Enrich the steps with details how to do them exactly. For example,
- How exactly to create a token in GitHub.
- On Mac - how exactly to paste the cookies into the secrets storage.

Record in `docs/agents/local_setup.md`: the bot account name and the commands
fetching the token and the cookie (or their plain text in case the user asked
for this). How to use them, is described in the relevant skills.

## Local setup file

The results of your survey must be persisted in the
`docs/agents/local_setup.md` file. The file MUST be loaded when you plan to
operate in this codebase not just in read-only mode.

If the file is missing or outdated and you intend to build or update it, tell
the user explicitly.

If the local setup file seems outdated (for instance, user frequently corrects
you or asks to do some specific missing things regularly), you MUST suggest
the user to update this file.

When updating or creating the local setup file, you MUST save the updates
BEFORE doing anything else, to not lose the context later.

---

If anything in this document seems outdated from how the code actually works,
then it must be immediately flagged to the user.
