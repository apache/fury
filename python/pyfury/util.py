import importlib
import inspect
import pkgutil
import sys
from typing import Dict, Callable

from pyfury._util import get_bit, set_bit, clear_bit, set_bit_to


# Copied from https://github.com/mars-project/mars/blob/master/mars/utils.py
# licensed at apache 2.0
def lazy_import(
    name: str,
    package: str = None,
    globals_: Dict = None,  # pylint: disable=redefined-builtin
    locals_: Dict = None,  # pylint: disable=redefined-builtin
    rename: str = None,
    placeholder: bool = False,
):
    rename = rename or name
    prefix_name = name.split(".", 1)[0]
    globals_ = globals_ or inspect.currentframe().f_back.f_globals

    class LazyModule(object):
        def __init__(self):
            self._on_loads = []

        def __getattr__(self, item):
            if item.startswith("_pytest") or item in ("__bases__", "__test__"):
                raise AttributeError(item)

            real_mod = importlib.import_module(name, package=package)
            if rename in globals_:
                globals_[rename] = real_mod
            elif locals_ is not None:
                locals_[rename] = real_mod
            ret = getattr(real_mod, item)
            for on_load_func in self._on_loads:
                on_load_func()
            # make sure on_load hooks only executed once
            self._on_loads = []
            return ret

        def add_load_handler(self, func: Callable):
            self._on_loads.append(func)
            return func

    if pkgutil.find_loader(prefix_name) is not None:
        return LazyModule()
    elif placeholder:
        return ModulePlaceholder(prefix_name)
    else:
        return None


class ModulePlaceholder:
    def __init__(self, mod_name: str):
        self._mod_name = mod_name

    def _raises(self):
        raise AttributeError(f"{self._mod_name} is required but not installed.")

    def __getattr__(self, key):
        self._raises()

    def __call__(self, *_args, **_kwargs):
        self._raises()


is_little_endian = sys.byteorder == "little"


__all__ = [
    "get_bit",
    "set_bit",
    "clear_bit",
    "set_bit_to",
    "lazy_import",
    "is_little_endian",
]
