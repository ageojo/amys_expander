env:
	python3 -m venv env

deps: env
	env/bin/pip install -U -r requirements/base.txt

clean:
	rm -rf env

run:
	env/bin/python bitly.py

.PHONY: env deps clean run
