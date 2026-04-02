from dataclasses import dataclass
from inspect import signature
from typing import Callable
from typing import Concatenate
from typing import Generic
from typing import ParamSpec
from typing import TypeVar
from typing import overload

A = TypeVar("A")
R = TypeVar("R")
P = ParamSpec("P")


@dataclass
class Curried(Generic[A, P, R]):
    """
    Models a curried function.

    The argument that may be missing is necessarily the first one. All the remaining
    arguments are mandatory.

    Notes
    -----
        VERY IMPORTANT!!!!!: The first argument of a curry-decorated function call
        cannot be named. In this case, a TypeError exception will be thrown.
    """

    fn: Callable[Concatenate[A, P], R]

    @overload
    def __call__(self, a: A, *args: P.args, **kwargs: P.kwargs) -> R: ...

    @overload
    def __call__(
        self, a: None = None, *args: P.args, **kwargs: P.kwargs
    ) -> Callable[[A], R]: ...

    def __call__(
        self, a: A | None = None, *args: P.args, **kwargs: P.kwargs
    ) -> Callable[[A], R] | R:
        fn_args = signature(self.fn).parameters.keys()
        first_arg_name = next(iter(fn_args))

        if a is not None:
            return self.fn(a, *args, **kwargs)
        elif len(args) + len(kwargs) == len(fn_args) and "a" not in fn_args:
            raise TypeError(
                f"parameter '{first_arg_name}' of '{self.fn.__name__}' cannot be named"
            )

        if first_arg_name in kwargs:
            return self.fn(*args, **kwargs)  # type:ignore

        for kwarg in kwargs:
            if kwarg not in fn_args:
                raise TypeError(
                    f"{self.fn.__name__}() got an unexpected keyword argument: '{kwarg}'"
                )

        def inner(a: A, /) -> R:
            return self.fn(a, *args, **kwargs)

        return inner


def curry(fn: Callable[Concatenate[A, P], R]) -> Curried[A, P, R]:
    """
    Converts the function passed as argument to an equivalent curried form. That is,
    it generates a function `g` that accepts all but the first argument of `fn`.

    Useful as a function annotation.

    # Examples

    >>> @curry
    >>> def root(x: float, index: float) -> float:
    >>>     return x**(1/n)
    >>>
    >>> square_root = root(index=2.0)
    >>> square_root(4.0)
    2.0
    >>>
    >>> cube_root = root(index=3.0)
    >>> cube_root(8.0)
    2.0
    >>>
    >>> root(4.0, index=2.0)
    2.0
    >>> root(x=4.0, index=2.0)
    TypeError: parameter 'x' of 'root' cannot be named

    Notes
    -----
        VERY IMPORTANT!!!!!: The first argument of a curry-decorated function call
        cannot be named. In this case, a TypeError exception will be thrown.
    """
    return Curried(fn)
