from dbk import errors

from ._providers import Provider, SyncContext
from .bofa import BofaProvider

_providers: dict[str, type[Provider]] = {
    BofaProvider.provider_id(): BofaProvider,
}


def get_providers() -> list[Provider]:
    return [p() for p in _providers.values()]


def find_provider(provider_id: str) -> Provider:
    if p := _providers.get(provider_id):
        return p()
    else:
        raise errors.DbkError(f"Unknown provider {provider_id}")
