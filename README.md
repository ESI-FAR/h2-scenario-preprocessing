## Offshore H2 scenario explorer

### How to use

```shell
$ python -m venv --prompt h2scenarios venv
$ source venv/bin/activate
$ pip install -e .
```

### Data

Datasets are assumed to be under `data/`.  Since they can be large,
you have to add them manually under this directory, and try to keep
the file names same as the original; e.g.:

```shell
$ tree -d data
data
└── D-Off_results
    ├── 100
    │   ├── ALK
    │   └── PEM
    ├── 200
    │   ├── ALK
    │   └── PEM
    ├── 300
    │   ├── ALK
    │   └── PEM
    └── 50
        ├── ALK
        └── PEM
```
