.PHONY: init clean run

init:
	mkdir -p images

run:
	pipenv run ./dl.py

clean:
	find images -depth 1 -print0 | xargs -0 rm -r --

