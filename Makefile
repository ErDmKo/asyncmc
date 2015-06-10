PYTHON=python
PYFLAKES=pyflakes

pep:
	pep8 asyncmc examples tests

test27: pep
	$(PYTHON) -m unittest discover -v $(FILTER)

test: pep 
	$(PYTHON) -m unittest discover -v $(FILTER)
