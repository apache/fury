class FuryError(Exception):
    pass


class ClassNotCompatibleError(FuryError):
    pass


class CompileError(FuryError):
    pass
