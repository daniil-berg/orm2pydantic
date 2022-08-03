from importlib import import_module


def resolve_dotted_path(dotted_path: str) -> object:
    """
    Resolves a dotted path to a global object and returns that object.

    Algorithm shamelessly stolen from the `logging.config` module from the standard library.
    """
    names = dotted_path.split('.')
    module_name = names.pop(0)
    found = import_module(module_name)
    for name in names:
        try:
            found = getattr(found, name)
        except AttributeError:
            module_name += f'.{name}'
            import_module(module_name)
            found = getattr(found, name)
    return found
