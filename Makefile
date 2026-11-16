PYTHON ?= python3
.PHONY: test demo demo-align check
test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v
demo:
	PYTHONPATH=src $(PYTHON) examples/smoke.py
demo-align:
	PYTHONPATH=src $(PYTHON) examples/align_demo.py
check: test demo demo-align
