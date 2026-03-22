from importlib import import_module

__all__ = ["admin_app", "admin_api_bp", "create_admin_app", "register_admin_routes", "run_admin_api"]


def __getattr__(name: str):
    if name in __all__:
        module = import_module(".app", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
