<h1 align="center"> GCLOUD CONNECTORS </h1>
<p align="center">
  <em>Python utilities to simplify connection with Google APIs.</em>
</p>
---
<p align="center">
  <a href="https://pypi.org/project/gcloud-connectors/"><img src="https://img.shields.io/pypi/v/gcloud-connectors.svg" /></a>
  <a href="https://pypi.org/project/gcloud-connectors/"><img src="https://img.shields.io/pypi/dm/gcloud-connectors" /></a>
  <a href="https://pypi.org/project/gcloud-connectors/"><img src="https://img.shields.io/pypi/pyversions/gcloud-connectors.svg" /></a>
</p>

`gcloud-connectors` is a simple and efficient api wrapper to interact with Google APIs and Pandas

With a one line code change, it allows any Pandas user to take advandage of his
multi-core computer, while `pandas` uses only one core.

`gcloud-connectors` also offers nice progress bars (available on Notebook and terminal) to
get a rough idea of the remaining amount of computation to be done.

| Without gcloud-connectors  | ![Without gcloud-connectors](https://github.com/pualien/py-gcloud-connectors/blob/master/images/df-from-bigquery.gif?raw=true) |
|:--------------------------:|--------------------------------------------------------------------------------------------------------------------------------|
| **With gcloud-connectors** | ![With gcloud-connectors](https://github.com/pualien/py-gcloud-connectors/blob/master/images/df-to-gstorage.gif?raw=true)            |

## Features


## Requirements

On **Linux** **macOS** & **Windows**, no special requirement rather than 
```sh
pip install gcloud-connectors
```

Example:

**✅ Tested on Mac and Linux**

```Python
import ...

```

!!! warning

    Enviroment Variable is required to interact with any of Google wrapped classes

## When should I use `gcloud-connectors`, `google-api-python` or `bigquery`?

> `pandas` is a fast, powerful, flexible and easy to use open source data analysis and
> manipulation tool, built on top of the Python programming language.
