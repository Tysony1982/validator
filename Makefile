.PHONY: docs

 docs:
	@sphinx-build -b html docs docs/_build
