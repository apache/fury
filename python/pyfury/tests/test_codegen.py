import os
import textwrap
import uuid

from pyfury import codegen


def test_debug_compiled():
    code = textwrap.dedent(
        """
    def _debug_compiled(x):
        print(x)
        print(x)
        return x
    """
    )[1:]
    unique_filename = f"_debug_compiled_{uuid.uuid4()}.py"
    with open(unique_filename, "w") as f:
        f.write(code)
    compiled = compile(code, unique_filename, "exec")
    context = {}
    exec(compiled, context, context)
    _debug_compiled = context["_debug_compiled"]
    assert _debug_compiled(2) == 2
    os.remove(unique_filename)


def test_compile_function():
    code, func = codegen.compile_function(
        "test_compile_function", ["x"], ["print(1)", "print(2)", "return x"], {}
    )
    print(code)
    assert func(100) == 100
