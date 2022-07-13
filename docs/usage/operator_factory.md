# Operator Factory

```{py:py:currentmodule} data_dag.operator_factory
```

See {py:class}`.OperatorFactory`

(custom_constructor)=
## Re-Interpreting Data

Ultimately, each object is created by its constructor being called. Since each factory and component is fundamentally a :py:class:`pydantic.BaseModel` object, its fields are defined at the class level, but how those fields get populated is customizable.

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

Note that, in many cases, :py:mod:`pydantic` may already have features that allow a variety of tweaks without needing a custom constructor. Single- and multi-field validation and transformation, amongst other features, are well-supported.

## Components

Currently, the only significant difference between :py:class:`OperatorComponent` and :py:class:`OperatorFactory` is that the latter defines a standard interface for how the operator should be constructed from the metadata. This encourages standardization and reusability.

However, since there is no standard way in which factories are linked together, outside of whatever constructs you define (e.g. in a :py:class:`~data_dag.dag_factory.DagFactory`), the different is currently merely semantic. This may change in the future as standard and third-party patterns become available, so in general, the interface should be followed where appropriate.
