.PHONY: check complexity fmt lint test

# The checks come in levels, cheapest first; AGENTS.md says when to run which.
# They all build with the same flags, so they share one build cache.
lint test check: export RUSTFLAGS = -Dwarnings

# Level 1: lints of every target, the complexity gate, and spelling (typos, which skips hidden
# folders unless named: a right word it flags goes in _typos.toml).
lint: complexity
	cargo clippy --workspace --all-targets
	typos . .agents

# Level 2, with CRATES: the unit tests of those crates (`make test CRATES="herdr-bus"`).
# Level 3, without: every unit test of the workspace, doc tests included.
# HERDR_BUS_DB names the user's bus file when set: the tests run without it, so none reaches it.
CRATES ?=
NO_USER_BUS = env -u HERDR_BUS_DB
test:
ifeq ($(strip $(CRATES)),)
	$(NO_USER_BUS) cargo test --workspace --doc
	$(NO_USER_BUS) cargo nextest run --workspace --no-tests=pass
else
	$(NO_USER_BUS) cargo nextest run --no-tests=pass $(foreach crate,$(CRATES),-p $(crate))
endif

# Formats the code, once a change is complete. No check fails on formatting.
fmt:
	cargo fmt --all

# Every level in one run: the gate of a change (AGENTS.md, step 5).
check: lint
	$(MAKE) test CRATES=

complexity:
	@report="$$(cccc --lang rust crates | jq -r '[.files[] | .path as $$path | .functions[] | recurse(.children[]?) | select(.cyclomatic > 10 or .cognitive > 15) | { path: $$path, line, name, cognitive, cyclomatic }] | sort_by([-.cyclomatic, -.cognitive, .path, .line]) | if length == 0 then empty else ("Cognitive\tCyclomatic\tFunction", (.[] | "\(.cognitive)\t\(.cyclomatic)\t\(.path):\(.line) \(.name)")) end')" || exit; \
	if [ -n "$$report" ]; then printf '%s\n' "$$report"; exit 1; fi
