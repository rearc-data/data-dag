# Operator Factory

```{py:currentmodule} data_dag.operator_factory
```

See {py:class}`~data_dag.operator_factory.OperatorFactory`

For an example, see [the getting started guide](operator_factory).

(custom_constructor)=
## Re-Interpreting Data

Ultimately, each object is created by its constructor being called. Since each factory and component is fundamentally a {py:class}`pydantic.BaseModel` object, its fields are defined at the class level, but how those fields get populated is customizable.

Let's say you have an old class like this::

    class Url(OperatorComponent):
        url: str

and then, at some point, you decide you want to update it to store the pieces of the url, like this::

    class Url(OperatorComponent):
        scheme: str
        host: str
        path: str
        params: Dict[str, Any] = {}

To avoid breaking existing code or data, you can write a custom constructor to permit old-style data definitions to continue to work::

    class Url(OperatorComponent):
        ...

        def __init__(self, url):
            parsed = urllib.parse.urlparse(url)
            super().__init__(scheme=parsed.scheme, host=parsed.host, ...)

This allows separating the input data definition for a type from its internal representation.

Note that, in many cases, {py:mod}`pydantic` may already have features that allow a variety of tweaks without needing a custom constructor. Single- and multi-field validation and transformation, amongst other features, are well-supported.

(component)=
## Components

{py:class}`OperatorFactory` defines a standard interface for how the operator should be constructed from the metadata, and provides the `task_id` attribute by default to facilitate that. {py:class}`OperatorComponent`, on the other hand, is currently just a proxy for {py:class}`pydantic.BaseModel` to encourage a consistent pattern for sharing data and validation across a DAG. This may change in the future.

## Task ID's and Groups

{py:class}`OperatorFactory` provides some pre-baked tooling to help define Task IDs.

- {py:class}`OperatorFactory` inherently has an optional `task_id: Optional[str]` attribute which can be set in data. To disallow using this, you can define a validator function that ensures this is empty.
- The {py:meth}`~OperatorFactory.default_task_id` property can be overridden to provide a default value if no task ID is specified. This is recommended to implement where practical.
- The {py:meth}`~OperatorFactory.get_task_id` method provides either the custom task ID, if provided, or the default if possible.

If implementing an operator pattern wrapped as a group (using {py:meth}`~OperatorFactory._make_operators`), then the task ID is used as the group ID, and the inner operator task ID's need only be unique within that context.
