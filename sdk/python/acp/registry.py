"""Component registry: id -> ComponentDef, with descriptor projection."""

from typing import Dict, List, Optional

from .codec import is_valid_component_id
from .component import ComponentDef, to_descriptor


class Registry:
    """Registered component definitions keyed by component id (spec §7.1)."""

    def __init__(self) -> None:
        self._components: Dict[str, ComponentDef] = {}

    def register(self, def_: ComponentDef) -> ComponentDef:
        if not is_valid_component_id(def_.id):
            raise ValueError("invalid component id: {} (spec §7.1)".format(def_.id))
        if not callable(def_.handle):
            raise ValueError("component {}: handle must be callable".format(def_.id))
        if def_.id in self._components:
            raise ValueError("component already registered: {}".format(def_.id))
        self._components[def_.id] = def_
        return def_

    def get(self, component_id: str) -> Optional[ComponentDef]:
        return self._components.get(component_id)

    def has(self, component_id: str) -> bool:
        return component_id in self._components

    def list(self) -> List[ComponentDef]:
        return list(self._components.values())

    def descriptors(self) -> List[dict]:
        return [to_descriptor(d) for d in self._components.values()]

    def __len__(self) -> int:
        return len(self._components)
