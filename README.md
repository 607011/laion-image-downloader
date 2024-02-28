# LAION image downloader


## Prerequisites

 - Python 3.10+

```
pipenv install
```

## Download metadata

```
wget -l1 -r --no-parent https://the-eye.eu/public/AI/cah/laion400m-met-release/laion400m-meta/ -P data --cut-dirs 6
```

## Download images referenced in metadata

```
pipenv run ./dl.py
```




## License

See [LICENSE](LICENSE)
