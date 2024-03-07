# LAION image downloader

*Downloader for images referenced by [LAION](https://laion.ai/) parquet files*


## Prerequisites

 - Python 3.10+

```
pipenv install
```

## Download metadata

Download parquet file with [400 million references](https://laion.ai/blog/laion-400-open-dataset/) to (probably copyrighted!) images:

```
wget -l1 -r --no-parent https://the-eye.eu/public/AI/cah/laion400m-met-release/laion400m-meta/ -P data --cut-dirs 6
```

There are even datasets with [2 to 5 billion references](https://laion.ai/blog/laion-5b/).

## Download images referenced in metadata

For instance, download all images tagged with the words “pumpkin” and “halloween” and store them along with the retrieved metadata in a new parquet file named “halloween-pumpkin-400m.parquet”:

```
pipenv run ./dl.py \
  the-eye.eu/*.parquet \
  --keywords pumpkin,halloween \
  --output halloween-pumpkin-400m.parquet
```


## Generate catalog

Generate an HTML file with a catalog of all or a selection of the downloaded images contained in the parquet file:

```
pipenv run ./mkcatalog.py \
  halloween-pumpkin-400m.parquet \
  --output pumpkins.html
```


## License

See [LICENSE](LICENSE)
