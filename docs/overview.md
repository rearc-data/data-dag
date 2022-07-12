# Overview

`data-dag` provides a variety of tools to simplify the process of writing data-defined Airflow DAGs.

This approach is incredibly useful when writing a large number of DAGs that rely, either in part or entirely, on shareable primitives. A primitive may or may not be a single Airflow operator. For example, you might want the following primitive to only create a table if it doesn't exist:

```{mermaid}
graph LR
    a["Check if table exists"] --> b["Create table (all_failed)"] --> c["Done (one_success)"]
    a --> c
```

While it is certainly possible to write a more complicated operator that performs this logic, it's often easier, clearer, and more maintainable to work with existing operators. Additionally, writing factories like this is easy to stack to build multiple levels of abstractions, or to re-use components across multiple different abstractions.

This pattern, in addition to simplifying and standardizing how multiple DAGs can be defined, also provides a framework for writing re-usable abstractions and operator compositions (for example, as a shareable plugin). This is because factories defined using `data-dag` can be defined in-code too, not just using data.

`data-dag` is written on top of the excellent [`pydantic` library](https://pydantic-docs.helpmanual.io/), providing automatic type coercion, field validation, and serialization and deserialization against a variety of data formats.

## Example

Re-usable operator and DAG template can be stored in a central location, such as a custom Airflow plugin (or a package within `dags/` works fine too):

(example_dag)=
```python
# plugins/my_factories/download.py

from data_dag.operator_factory import OperatorFactory
from data_dag.dag_factory import DagFactory

from urllib.request import urlretrieve
from typing import List
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


class DownloadOperator(OperatorFactory):
    """An operator factory for safely downloading files to a known location"""

    name: str
    url: str
    path: str

    def make_operator(self):
        with TaskGroup(group_id=f'download_{self.name}') as group:
            check = HttpSensor(
                task_id='check_exists',
                endpoint=self.url
            )
            download = PythonOperator(
                task_id=f'download',
                python_callable=lambda: urlretrieve(self.url, self.path)
            )
            check >> download

        return group


class DownloaderDag(DagFactory):
    """A DAG factory for producing simple DAGs that just download a bunch of files"""

    downloads: List[DownloadOperator]

    def _make_dag(self):
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')

        for download in self.downloads:
            start >> download.make_operator() >> end
```

Then a definition for a particular DAG can live in a data file:

```yaml
# dags/yaml/sample_dag.yaml

dag_id: sample_dag
description: An example of how to write a data-driven DAG
schedule_interval: '@daily'
start_date: '2020-01-01T00:00:00'
downloads:
- name: data
  url: https://www.example.com/data.zip
  path: data.zip
- name: manifest
  url: https://www.example.com/manifest.json
  path: manifest.json
```

That data file can then be loaded into a DAG. Per Airflow's requirements, this must be done in a Python file located in `dags/` and the result must be saved into a uniquely named global variable. The simplest possible example is this:

```python
# dags/sample_dag.py

from yaml import safe_load
from my_factories.download import DownloaderDag

with open('yaml/sample_dag.yaml', 'r') as f:
    dag_data = safe_load(f)

dag = DownloaderDag.parse_obj(dag_data).make_dag()
```

![img.png](_images/img.png)

Multiple DAGs
-------------

Obviously, using a template isn't much use if you only fill it in once. Here's a simple example of a loader that will load any number of YML files from a folder and publish each one as a DAG in Airflow:

(loader_script)=
```python
# dags/load_yml_files.py

from pathlib import Path
from airflow import DAG
import yaml
from my_factories import BaseDag

dag_dir = Path(__file__).parent

# For each YAML file in a particular directory...
for yaml_file_path in dag_dir.glob('typical_dags/**.yml'):
    with open(yaml_file_path) as f:
        dag_metadata = yaml.safe_load(f)

    # ... generate a DAG from that metadata
    dag_metadata_obj = BaseDag.parse_obj(dag_metadata)
    dag = dag_metadata_obj.make_dag()

    # See https://www.astronomer.io/guides/dynamically-generating-dags/
    globals()[dag_metadata_obj.dag_id] = dag
```
