import abc
import inspect

import dacite


class LazyDaciteConfig:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __get__(self, obj, objtype):
        return dacite.Config(
            cast=objtype._dacite_cast(),
            type_hooks=objtype._dacite_type_hooks(),
            forward_references=objtype._dacity_forward_references(),
            **self.kwargs,
        )
