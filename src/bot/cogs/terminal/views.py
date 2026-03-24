from typing import Optional
import io

import discord

from .render import build_terminal_message_payload_async
from .router import execute_terminal_action
from .state import TerminalState


TERMINAL_TIMEOUT_SECONDS = 900


class TerminalInputModal(discord.ui.Modal):
    def __init__(self, title: str, label: str):
        super().__init__(title=title)
        self.value_input = discord.ui.TextInput(
            label=label,
            required=True,
            max_length=120,
            placeholder="Enter value and submit",
        )
        self.add_item(self.value_input)
        self.submitted_value: Optional[str] = None

    async def on_submit(self, interaction: discord.Interaction):
        self.submitted_value = str(self.value_input.value)
        await interaction.response.defer(ephemeral=True, thinking=False)


class TerminalRefreshView(discord.ui.View):
    def __init__(self, requester_id: int, source_view: "TerminalView"):
        super().__init__(timeout=TERMINAL_TIMEOUT_SECONDS)
        self.requester_id = requester_id
        self.source_view = source_view

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary)
    async def refresh_terminal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the requester can refresh this terminal.", ephemeral=True)
            return

        new_view = TerminalView(self.source_view.bot, self.source_view.command_ctx, self.source_view.state)
        if not interaction.response.is_done():
            await interaction.response.defer(thinking=False)

        embed, file = await build_terminal_message_payload_async(self.source_view.state)
        kwargs = {"embed": embed, "view": new_view}
        if file:
            kwargs["attachments"] = [file]
        await interaction.edit_original_response(**kwargs)
        new_view.message = interaction.message


class TerminalView(discord.ui.View):
    def __init__(self, bot, command_ctx, state: TerminalState):
        super().__init__(timeout=TERMINAL_TIMEOUT_SECONDS)
        self.bot = bot
        self.command_ctx = command_ctx
        self.state = state
        self.message: Optional[discord.Message] = None
        self._action_in_progress = False

    async def _redraw(self, interaction: discord.Interaction, defer_first: bool = True) -> None:
        if defer_first and not interaction.response.is_done():
            await interaction.response.defer(thinking=False)

        embed, file = await build_terminal_message_payload_async(self.state)
        kwargs = {"embed": embed, "view": self}
        if file:
            kwargs["attachments"] = [file]
        await interaction.edit_original_response(**kwargs)

    def _authorized(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.state.requester_id

    async def _begin_action(self, interaction: discord.Interaction) -> bool:
        if self._action_in_progress:
            await interaction.response.send_message(
                "Terminal is processing the previous action. Try again in a second.",
                ephemeral=True,
            )
            return False
        self._action_in_progress = True
        return True

    def _end_action(self) -> None:
        self._action_in_progress = False

    async def on_timeout(self):
        if not self.message:
            return
        try:
            refresh_view = TerminalRefreshView(self.state.requester_id, self)
            await self.message.edit(view=refresh_view)
        except Exception:
            return

    @discord.ui.button(label="Up (Prev)", style=discord.ButtonStyle.secondary)
    async def up(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return
        if not await self._begin_action(interaction):
            return
        self.state.move_up()
        try:
            await self._redraw(interaction)
        finally:
            self._end_action()

    @discord.ui.button(label="Down (Next)", style=discord.ButtonStyle.secondary)
    async def down(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return
        if not await self._begin_action(interaction):
            return
        self.state.move_down()
        try:
            await self._redraw(interaction)
        finally:
            self._end_action()

    @discord.ui.button(label="Select (Open/Run)", style=discord.ButtonStyle.success)
    async def select(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return
        if not await self._begin_action(interaction):
            return

        try:
            item = self.state.current_item()
            self.state.last_error = ""

            if item.submenu:
                self.state.enter_submenu(item.submenu)
                await self._redraw(interaction)
                return

            if not item.action:
                self.state.last_output = "No action available"
                await self._redraw(interaction)
                return

            user_input = ""
            if item.requires_input:
                modal = TerminalInputModal(title="TERMINAL INPUT", label=item.input_hint or "Input")
                await interaction.response.send_modal(modal)
                await modal.wait()
                user_input = (modal.submitted_value or "").strip()
                if not user_input:
                    self.state.last_error = "Input cancelled or empty"
                    followup_embed, followup_file = await build_terminal_message_payload_async(self.state)
                    kwargs = {"embed": followup_embed, "view": self}
                    if followup_file:
                        kwargs["attachments"] = [followup_file]
                    await interaction.message.edit(**kwargs)
                    return

                self.state.last_output = f"Running {item.label}..."
                followup_embed, followup_file = await build_terminal_message_payload_async(self.state)
                kwargs = {"embed": followup_embed, "view": self}
                if followup_file:
                    kwargs["attachments"] = [followup_file]
                await interaction.message.edit(**kwargs)

                try:
                    result = await execute_terminal_action(self.bot, self.command_ctx, item.action, user_input)
                    self.state.last_output = result
                except Exception as exc:
                    self.state.last_error = str(exc)
                    self.state.last_output = "Action failed"

                followup_embed, followup_file = await build_terminal_message_payload_async(self.state)
                kwargs = {"embed": followup_embed, "view": self}
                if followup_file:
                    kwargs["attachments"] = [followup_file]
                await interaction.message.edit(**kwargs)
                return

            self.state.last_output = f"Running {item.label}..."
            await self._redraw(interaction)

            try:
                result = await execute_terminal_action(self.bot, self.command_ctx, item.action, user_input)
                self.state.last_output = result
            except Exception as exc:
                self.state.last_error = str(exc)
                self.state.last_output = "Action failed"

            followup_embed, followup_file = await build_terminal_message_payload_async(self.state)
            kwargs = {"embed": followup_embed, "view": self}
            if followup_file:
                kwargs["attachments"] = [followup_file]
            await interaction.message.edit(**kwargs)
        finally:
            self._end_action()

    @discord.ui.button(label="Back (Parent)", style=discord.ButtonStyle.danger)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return
        if not await self._begin_action(interaction):
            return
        self.state.go_back()
        try:
            await self._redraw(interaction)
        finally:
            self._end_action()

    @discord.ui.button(label="Print Output", style=discord.ButtonStyle.primary, row=1)
    async def print_output(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return

        output = (self.state.last_output or "").strip()
        if not output:
            await interaction.response.send_message("No terminal output to print yet.", ephemeral=True)
            return

        title = "Terminal Output"
        current_item = self.state.current_item().label if self.state.current_menu() else "N/A"
        header = (
            f"**{title}**\n"
            f"Screen: `{self.state.menu_key.upper()}`\n"
            f"Selection: `{current_item}`"
        )

        if len(output) <= 1800:
            await interaction.response.send_message(f"{header}\n```\n{output}\n```", ephemeral=False)
            return

        file = discord.File(io.BytesIO(output.encode("utf-8")), filename="terminal_output.txt")
        await interaction.response.send_message(
            f"{header}\nOutput was too long for inline display; attached as file.",
            file=file,
            ephemeral=False,
        )
