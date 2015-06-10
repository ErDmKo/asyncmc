PYTHON=python
PYFLAKES=pyflakes

flake:
	$(PYFLAKES) .

pep:
	pep8 asyncmc examples tests

test27: pep 
	$(PYTHON) -m unittest discover -v
test: pep 
	$(PYTHON) runtests.py -v 5 $(FILTER)

testloop: pep flake
	$(PYTHON) runtests.py --forever $(FILTER)
