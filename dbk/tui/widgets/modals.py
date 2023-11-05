from typing import Any

from pydantic import BaseModel
from textual.app import ComposeResult
from textual.containers import Grid, Vertical
from textual.events import Key
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Select, Static

from dbk.core import providers

from ..models.modals import CreateAccountArgs, CreateConnectionArgs


class NewConnectionModal(ModalScreen[CreateConnectionArgs | None]):
    def compose(self):
        with Vertical(id="dialog"):
            yield Static("Create New Connection", id="title")
            yield Static("", id="error")
            yield Select(
                [
                    (provider.provider_name(), provider.provider_id())
                    for provider in providers.get_providers()
                ],
                prompt="Select Provider",
                id="select-provider",
            )
            yield Input(placeholder="Connection Name", id="name")

            with Vertical(id="provider-content"):
                pass

            with Grid(id="modal-buttons"):
                yield Button("Cancel", id="cancel")
                yield Button("Create", id="create")

    async def on_select_changed(self, e: Select.Changed):
        if e.select.id != "select-provider":
            return

        content = self.query_one("#provider-content", Vertical)
        await content.remove_children()

        provider_id = e.value
        if not isinstance(provider_id, str):
            return

        provider = providers.find_provider(provider_id)
        model: type[BaseModel] = provider.custom_data_model()
        schema = model.model_json_schema()
        content.mount_all(self.schema_to_form_widgets(schema))

    def on_button_pressed(self, e: Button.Pressed):
        error = self.query_one("#error", Static)

        match e.button.id:
            case "cancel":
                self.dismiss(None)
            case "create":
                name = self.query_one("#name", Input).value
                provider_id = self.query_one("#select-provider", Select).value
                provider_data: dict[str, Any] = {
                    c.id: c.value  # type: ignore
                    for c in self.query_one("#provider-content", Vertical).children
                }

                if not name:
                    error.update("Name is required")
                    return

                if not provider_id:
                    error.update("Provider is required")
                    return

                self.dismiss(
                    CreateConnectionArgs(
                        name=name,
                        provider_id=provider_id,  # type: ignore
                        provider_data=provider_data,
                    )
                )

    @staticmethod
    def schema_to_form_widgets(schema: dict[str, Any]) -> ComposeResult:
        props = schema["properties"]
        for prop_name, prop_schema in props.items():
            if prop_type_ref := prop_schema.get("$ref"):
                prop_type = prop_type_ref.split("/")[-1]
                prop_schema = schema["$defs"][prop_type]

            prop_title = prop_schema.get("title", prop_name)

            match prop_schema["type"]:
                case "string":
                    if prop_enum_vals := prop_schema.get("enum"):
                        yield Select(
                            [(v, v) for v in prop_enum_vals],
                            id=prop_name,
                            prompt=prop_title,
                        )
                    else:
                        yield Input(placeholder=prop_title, id=prop_name)


class NewAccountModal(ModalScreen[CreateAccountArgs | None]):
    def __init__(self, parent_id: int, is_group: bool, *args, **kwargs):
        self.parent_id = parent_id
        self.is_group = is_group
        super().__init__(*args, **kwargs)

    def compose(self):
        with Vertical(id="dialog"):
            if self.is_group:
                yield Static("Create New Group")
            else:
                yield Static("Create New Account")
            yield Static("", id="error")
            yield Input(placeholder="Name", id="name")
            with Grid(id="modal-buttons"):
                yield Button("Cancel", id="cancel")
                yield Button("Create", id="create")

    def on_key(self, e: Key):
        if e.key == "escape":
            e.stop()
            return self._cancel()
        if e.key == "enter":
            e.stop()
            return self._create()

    def on_button_pressed(self, e: Button.Pressed):
        match e.button.id:
            case "cancel":
                e.stop()
                return self._cancel()
            case "create":
                e.stop()
                return self._create()

    def _cancel(self):
        self.dismiss(None)

    def _create(self):
        name = self.query_one("#name", Input).value
        if not name:
            self.query_one("#error", Static).update("Name is required")
            return
        self.dismiss(
            CreateAccountArgs(
                parent_id=self.parent_id,
                name=name,
                create_group=self.is_group,
            )
        )
