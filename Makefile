PYTHON=python
PYFLAKES=pyflakes

pep:
	pep8 asyncmc examples tests

flake:
	$(PYFLAKES) asyncmc examples tests

test27: pep flake
	$(PYTHON) -m unittest discover -v $(FILTER)

test: pep flake
	$(PYTHON) -m unittest discover -v $(FILTER)

cov: pep flake
	coverage run -m unittest discover && coverage report -m
