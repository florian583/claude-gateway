# Contributing

## Keep Git identity private

Copy your exact GitHub no-reply address from [Settings → Emails](https://github.com/settings/emails). Use your GitHub handle as your Git name. Configure this clone, not every repository:

```sh
git config --local user.name YOUR_GITHUB_HANDLE
git config --local user.email YOUR_GITHUB_NOREPLY_ADDRESS
git config --local user.useConfigOnly true
make hooks
```

On GitHub, enable **Keep my email addresses private** and **Block command line pushes that expose my email**. GitHub's push protection checks the most recent commit against your private account emails; it does not replace an audit of every commit. See [GitHub's email privacy documentation](https://docs.github.com/en/account-and-profile/how-tos/email-preferences/blocking-command-line-pushes-that-expose-your-personal-email-address).

The repository hooks check the effective author and committer identity, staged contents, commit messages, and reachable history before pushing. A no-reply address is still visible metadata, but is not a personal mailbox. Changing Git configuration does not change old commits.

Hooks are local and must be installed after each clone. They can be bypassed, so CI checks the published history too. CI is detection after upload, not protection against initial disclosure. Never use `--no-verify` to bypass a privacy failure.

## Keep fixtures synthetic

Use role-based names such as `primary`, `secondary`, and `backup`, with fake IDs such as `account-a`. Do not copy real account names, contact details, home paths, tokens, settings, logs, snapshots, or credentials.

Tests may use reserved example domains. Anthropic-shaped fixtures must contain the explicit `test-` marker and descriptive words, only in Go test files. The checker rejects common credential patterns, private email addresses, personal home paths, numbered account aliases, runtime files, and binary payloads. It prints locations and categories, not matched values. This is a guardrail, not proof that arbitrary PII or encoded secrets are absent; review staged changes too.

## Validate

```sh
make check build
```

Runtime settings belong outside the checkout. Python 3 is used only by development/privacy checks; the proxy itself requires no Python runtime.
