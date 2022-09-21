# Getting Started

If you are looking to write abstractions, or want to start converting existing DAGs into data-driven DAGs, start by [writing some abstractions](writing_abstractions).

If you already have some abstractions and are looking to start using them to define concrete DAGs, you can jump straight to [using abstractions](using_abstractions).

(writing_abstractions)=

## Writing Abstractions

There are primarily three tiers of abstractions:

- A [DAG factory](dag_factory)
- An [operator factory](operator_factory)
- A [component](component)

All three are interfaces over a `pydantic` model, and thus provide much of the same functionality. Let's explore their differences and where each one is helpful.

Common workflows that all abstractions share are:

- Can use `parse_obj` to instantiate the class from a dictionary of data (e.g., from a YAML or JSON file)
- Can also be directly constructed in-code, just like any other `pydantic`-based class
- Can have custom field validation that goes well beyond mere type verification or coercion (e.g. checking that a string matches a regex, or that a number is within a given range, or converting a string to an enum)
- `*args, **kwargs` can often be passed to the primary methods of the various factories; however, it is recommended to make each class's attributes self-contained such that the factory can be executed without any additional arguments.
- Any classes you write that should not be able to be instantiated should inherit {py:class}`abc.ABC`

(dag_factory)=
### DAG Factory

A {py:class}`~data_dag.dag_factory.DagFactory` constructs a complete DAG, and contains related DAG metadata (such as schedule, name, and start date). It can compose together operator factories, or it can be entirely self-container, or a mix.

```python
from typing import List
from data_dag.dag_factory import DagFactory
from data_dag.operator_factory import OperatorFactory


class SimpleChainDag(DagFactory):
    ops: List[OperatorFactory]

    def _make_dag(self, *args, **kwargs) -> None:
        operator_instances = [op.make_operator() for op in self.ops]
        for a, b in zip(operator_instances[:-1], operator_instances[1:]):
            a >> b
```

The base factory class provides definitions for virtually all attributes of {py:class}`~airflow.models.dag.DAG`, such that your custom factory classes need only define any additional attributes needed for your particular DAG. It also provides a convenience implementation of {py:meth}`~data_dag.dag_factory.DagFactory.make_dag` that is effectively the following:

```python
def make_dag(self, *args, **kwargs):
    with DAG(**self.dag_args) as dag:
        self._make_dag(*args, **kwargs)
    return dag
```

That is, your custom factory, in general, needs only define {py:meth}`~data_dag.dag_factory.DagFactory._make_dag` to create and link the operators needed for your DAG. See the [example DAG](example_dag) for reference.

(operator_factory)=
### Operator Factory

```{py:currentmodule} data_dag.operator_factory
```

An {py:class}`.OperatorFactory` use used to create one or more operators as a pattern. The result should generally be a single operator-like object, or a list (though your calling code will need to know how to handle this properly).

Let's create, as an example, a universal Docker abstraction. We'll start with a simple wrapper around a DockerOperator:

```python
from data_dag.operator_factory import OperatorFactory

from airflow.providers.docker.operators.docker import DockerOperator

class DockerContainer(OperatorFactory):
    name: str
    image: str
    command: str

    def make_operator(self):
        return DockerOperator(
            task_id=f'docker_run_{self.name}',
            image=self.image,
            command=self.command,
        )
```

To expand this, let's say we wish to re-use this factory to create a kubernetes operator. In this case, we pass kubernetes-specific configuration into the factory, but in practice this might be best handled using external configuration, such as an Airflow connection. We might get the following:

```python
...
from typing import Optional, Dict

class DockerContainer(OperatorFactory):
    ...
    kubernetes_config: Optional[Dict]

    def make_docker_operator(self):
        return DockerOperator(...)

    def make_kubernetes_operator(self):
        return KubernetesPodOperator(..., **self.kubernetes_config)

    def make_operator(self):
        return self.make_docker_operator()
```

We could then expose this additional functionality to datafiles by allowing a runtime switch of which operator, regular Docker or Kubernetes, we wish to use.

```python
import enum

class DockerRuntime(enum.Enum):
    LOCAL = 'docker'
    KUBERNETES = 'kubernetes'

class DockerContainer(OperatorFactory):
    ...
    runtime: DockerRuntime

    def make_operator(self):
        if self.runtime == DockerRuntime.LOCAL:
            return self.make_docker_operator()
        elif self.runtime == DockerRuntime.KUBERNETES:
            return self.make_kubernetes_operator()
```

This could be instantiated with the following data:

```yaml
image: python3
command: python -m "print('hello world!')"
runtime: docker
```

Let's say, for some reason, you want the ability to build a docker image before running it. This would be easy to integrate into this factory:

```python
from airflow.utils.task_group import TaskGroup

class DockerContainer(OperatorFactory):
    ...
    build_config: Optional[Dict]

    def make_build_operator(self):
        return BashOperator(
            task_id='docker build -t {self.image} ...'
        )

    def make_operator(self):
        with TaskGroup(group_id=f'run_{self.name}') as group:
            if self.runtime == DockerRuntime.LOCAL:
                run_operator = self.make_docker_operator()
            elif self.runtime == DockerRuntime.KUBERNETES:
                run_operator = self.make_kubernetes_operator()

            if self.build_config:
                build_operator = self.make_build_operator()
                build_operator >> run_operator

        return group
```

```{note}
In most cases, when creating multiple operators, you should wrap them up in a {py:class}`airflow.utils.task_group.TaskGroup` so that the result can be treated like a single operator. This makes single operators and compositions of operators indistinguishable from outside your factory, allowing both to be used identically.
```

This approach allows for factories to become flexible abstractions for common functionality. Additionally, if necessary, it would be straightforward to break an abstraction apart into a composition of multiple simpler factories.

(components)=
### Components

Not every abstraction directly maps to a DAG or operator, or even a collection of operators. For example, you might want an abstraction for a file in S3 that can be re-used in a variety of other operator factories. This can still be data-driven using {py:class}`.OperatorComponent`:

```python
from data_dag.operator_factory import OperatorComponent, OperatorFactory

class S3File(OperatorComponent):
    bucket: str
    key: str

class CopyFileToS3(OperatorFactory):
    local_file: str
    s3_file: S3File

    def make_operator(self):
        ...
```

In some cases, it may be desirable to create a very simple component or factory with a single required field. Consider the following trivial example:

```python
from data_dag.operator_factory import SimpleOperatorComponent, SimpleOperatorFactory

class Path(SimpleOperatorComponent):
    path: str
    is_file: bool = True

class CheckFileExists(SimpleOperatorFactory):
    file_path: Path

    def make_operator(self):
        return BashOperator(...)
```

By extending {py:class}`SimpleOperatorComponent` instead of the ordinary {py:class}`OperatorComponent`, this object can be defined in data using just a string rather than a fully dictionary:

```yaml
file_path: 'path/to/file.txt'

# is equivalent to
file_path:
  path: 'path/to/file.txt'
```

The same is true of {py:class}`.SimpleOperatorFactory` relative to {py:class}`.OperatorFactory`. In fact, since the above sample code uses both a {py:class}`.SimpleOperatorComponent` and a {py:class}`.SimpleOperatorFactory`, the entire operator can be defined using a string rather than a full dictionary:

```python
CheckFileExists.parse_obj('path/to/file.txt')
# CheckFileExists(file_path=Path(path='path/to/file.txt', is_file=True))
```

This allows, in many cases, creating fine-grained types to help keep data organized without sacrificing readability in the data needed to define those types:

```python
from pydantic import Field

class Schema(SimpleOperatorComponent):
    name: str

class Column(SimpleOperatorComponent):
    name: str
    type: str = 'VARCHAR'

class Table(OperatorFactory):
    schema_: Schema = Field(alias='schema')
    name: str
    columns: List[Column]
```

```yaml
schema: 'my_schema'
name: 'my_table'
columns:
- col1
- col2
- name: col3
  type: INTEGER
```

(using_abstractions)=
## Using abstractions

All abstractions in {py:mod}`data_dag` are {py:mod}`pydantic` models, meaning that all usual Pydantic features are available. This section is a quick introduction to how those features allow for making data-driven DAGs.

### Interacting with dictionaries

Several common data languages, such as YAML and JSON, serialize collections of simple objects, such as dictionaries, lists, numbers, and strings. Different markup languages support different types and features, so they may each be appropriate under different circumstances.

When these files are loaded into memory, e.g. using `json` or `pyyaml`, the result is an in-memory object comprised of simple data types. Beyond the most trivial use cases, these objects are typically nested and represent a variety of application-specific details. Working directly with these raw dictionaries and lists is likely to involve a lot of boilerplate code; however, if we can map this data into classes, creating useful object instances, we can tie data directly into the functionality related to it.

To do this mapping from in-memory data (no matter where it was loaded from or how it was constructed), `pydantic` allows us to use `MyType.parse_obj(data)` where `MyType` is the factory or component that is represented by the data.

```{note}
This approach requires that your full top-level data object maps directly onto a Python type. In general, this either isn't hard or is a good idea to add; however, you can also parse out pieces of the data, or restructure it, to match the data types you've defined, especially if you're working with a legacy or shared data schema that isn't easy to modify. Another option is to add [a custom `__init__`](custom_constructor) to a top-level type that knows how to interpret a slightly different input data schema.
```

For example, consider that you already have the following model:

```python
from data_dag.operator_factory import OperatorFactory
from data_dag.dag_factory import DagFactory

from typing import List

class EmailUser(OperatorFactory):
    email_address: str
    message_html: str

    def make_operator(self):
        ...

class EmailAllUsers(DagFactory):
    emails: List[EmailUser]
    email_upon_completion: EmailUser

    def _make_dag(self):
        ...
```

You could load an email list from the following YAML:
```yaml
dag_id: email_users
schedule_interval: '@weekly'

emails:
  - email_address: sample@example.com
    message_html: "<p>Hello, world!</p>"
  - email_address: sample2@example.com
    message_html: "<p>Hi, you're our second email</p>"
email_upon_completion:
  email_address: admin@my.site.com
  message_html: "<p>All messages sent</p>"
```

This would produce the following in-memory object:
```python
data = {
    'dag_id': 'email_users',
    'schedule_interval': '@weekly',
    'email_upon_completion': {
        'email_address': 'admin@my.site.com',
        'message_html': '<p>All messages sent</p>'
    },
    'emails': [
        {'email_address': 'sample@example.com', 'message_html': '<p>Hello, world!</p>'},
        {'email_address': 'sample2@example.com', 'message_html': "<p>Hi, you're our second email</p>"}
    ]
}
```

Which we can then parse as:
```python
dag_metadata = EmailAllUsers.parse_obj(data)
# EmailAllUsers(
#   dag_id='email_users',
#   ...
#   emails=[
#       EmailUser(
#           email_address='sample@example.com',
#           message_html='<p>Hello, world!</p>'),
#       EmailUser(
#           email_address='sample2@example.com',
#           message_html="<p>Hi, you're our second email</p>")
#   ],
#   email_upon_completion=EmailUser(
#       email_address='admin@my.site.com',
#       message_html='<p>All messages sent</p>'
#   )
# )
```

To finally convert that metadata object into an Airflow DAG, we can run `dag_metadata.make_dag()`. Typically, this would be done in a [loader script](loader_script) that's designed to interact cleanly with Airflow.

You can also generate these objects dynamically in-code:

```python
email_tasks = [
    EmailUser(email_address=user.email, message_html=message_template.render(user))
    for user in User.objects.all()
]
admin_email_task = EmailUser(
    email_address=User.objects.get(admin=True),
    message_html=success_template.render(),
)
dag_metadata = EmailAllUsers(
    dag_id='email_users',
    schedule='@weekly',
    emails=email_tasks,
    email_upon_completion=admin_email_task,
)
```

Note that you can also use `pydantic` to map the resulting in-memory objects back into data, either into a dictionary or directly into JSON:

```python
dag_metadata.dict()
dag_metadata.json()
```

This additionally means that you can generate the DAG metadata in one location (say, nightly in a web application), save it out to a known location, then restore it from that location back into your Airflow server to be rendered as a functional DAG. This can be done without direct communication between your servers and without introducing the possibility of arbitrary code execution in your Airflow DAGs, since all code is self-contained and only validated parameters are allowed to be passed around.

```{warning}
While this is similar, for example, to using JSON serialization to define celery tasks, you must keep in mind that the final result will be entirely rendered, all at once and repeatedly every several seconds, in Airflow, and is thus subject to Airflow's scaling limitations. These limitations mean that individual DAGs should generally be kept fairly small (no more than a few hundred operators) and that the number of DAGs likewise should be kept limited, depending on the scale of your deployment.
```
