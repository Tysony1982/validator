.PHONY: docs linkcheck

docs:
	@sphinx-build -b html docs docs/_build

linkcheck:
	@sphinx-build -b linkcheck docs docs/_build
