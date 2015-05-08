PYTHON=python3
PYFLAKES=pyflakes

flake:
	$(PYFLAKES) .

test:
	$(PYTHON) runtests.py $(FILTER)

pep:
	pep8 asyncmc examples tests

testloop: pep flake
	$(PYTHON) runtests.py --forever $(FILTER)
