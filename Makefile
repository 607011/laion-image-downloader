.PHONY: init clean run

all: init clean run

fetch:
	wget -l1 -r --no-parent https://the-eye.eu/public/AI/cah/laion400m-met-release/laion400m-meta/ -P data --cut-dirs 6

init:
	mkdir -p images

run:
	pipenv run ./dl.py data/the-eye.eu/*.parquet --no-continue --keywords python,forest --output python.parquet

clean:
	rm -rf images
	mkdir images
