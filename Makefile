PYTHON=python3
PYFLAKES=pyflakes

flake:
	$(PYFLAKES) .

pep:
	pep8 asyncmc examples tests

test: pep flake
	$(PYTHON) runtests.py -v 5 $(FILTER)

testloop: pep flake
	$(PYTHON) runtests.py --forever $(FILTER)
