.PHONY: seed worker

seed:
	uv run python -m seed.seed

worker:
	uv run python -m worker
