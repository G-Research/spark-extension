# PySpark dependencies

Using PySpark on a cluster requires all cluster nodes to have those Python packages installed that are required by the PySpark job.
Such a deployment can be cumbersome, especially when running in an interactive notebook.

The `spark-extension` package allows installing Python packages programmatically by the PySpark application itself (PySpark ≥ 3.1.0).
These packages are only accessible by that PySpark application, and they are removed on calling `spark.stop()`.

```python
# noinspection PyUnresolvedReferences
from gresearch.spark import *

spark.install_pip_package("pandas", "pyarrow")
```

Above example installs PIP packages `pandas` and `pyarrow` via `pip`. Method `install_pip_package` takes any `pip` command line argument:

```python
# install packages with version specs
spark.install_pip_package("pandas==1.4.3", "pyarrow~=8.0.0")

# install packages from package sources (e.g. git clone https://github.com/pandas-dev/pandas.git)
spark.install_pip_package("./pandas/")

# install packages from git repo
spark.install_pip_package("git+https://github.com/pandas-dev/pandas.git@main")

# use a pip cache directory to cache downloaded whl files
spark.install_pip_package("pandas", "pyarrow", "--cache-dir", "/home/user/.cache/pip")

# use an alternative index url (other than https://pypi.org/simple)
spark.install_pip_package("pandas", "pyarrow", "--index-url", "https://artifacts.company.com/pypi/simple")

# install pip packages quietly (only disables output of PIP)
spark.install_pip_package("pandas", "pyarrow", "--quiet")
```
