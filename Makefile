.PHONY: test

test:
	uv run --group dev pytest -q Test
