# Wiki index

A playbook for working in this repository, one topic per page. `AGENTS.md` holds the rules that
always apply, says what earns a line here, and when to read a page and keep it current.

Elsewhere: the spec is [SPEC.md](../../SPEC.md); the coding standards are in
[CODING_STANDARDS.md](../../CODING_STANDARDS.md); the glossary is [CONTEXT.md](../../CONTEXT.md);
decisions go in `.agents/adr/`; the settings the skills read are in
[.agents/settings/](../settings/).

## Working in the repository

- [Checks](checks.md): gotchas of the check commands.
- [Checking from a shell](shell-checks.md): the user's checks of the bus, its command and a
  session, on a private bus file, and how the bus file's folder changes on a write.
- [Tests](unit-tests.md): the testing policy, tautological tests, how a test waits.
- [Parallel workspaces](parallel-workspaces.md): the dev shell and the gate in a jj workspace,
  and cleaning up.
