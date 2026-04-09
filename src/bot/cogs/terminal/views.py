from typing import Optional
import io
import asyncio

import discord

from src.bot.task_manager import terminal_task_manager
from src.config.settings import get_terminal_admin_password

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
        self._loading_task: Optional[asyncio.Task] = None
        self._active_terminal_task_id: Optional[str] = None

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

    async def _redraw_message(self) -> None:
        if not self.message:
            return
        embed, file = await build_terminal_message_payload_async(self.state)
        kwargs = {"embed": embed, "view": self}
        if file:
            kwargs["attachments"] = [file]
        await self.message.edit(**kwargs)

    async def _loading_ticker(self) -> None:
        try:
            while self.state.is_loading:
                await asyncio.sleep(0.6)
                if not self.state.is_loading:
                    break
                self.state.bump_loading_tick()
                await self._redraw_message()
        except asyncio.CancelledError:
            return
        except Exception:
            # Loading animation is best-effort and must never fail command execution.
            return

    async def _start_loading(self, label: str, stage: str = "Running") -> None:
        self.state.begin_loading(label=label, stage=stage)
        await self._redraw_message()
        if self._loading_task and not self._loading_task.done():
            self._loading_task.cancel()
        self._loading_task = asyncio.create_task(self._loading_ticker())

    async def _stop_loading(self) -> None:
        self.state.end_loading()
        if self._loading_task and not self._loading_task.done():
            self._loading_task.cancel()
            try:
                await self._loading_task
            except asyncio.CancelledError:
                pass
        self._loading_task = None

    async def _handle_progress_update(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return

        task_status: Optional[str] = None
        raw_status = payload.get("_status")
        if not isinstance(raw_status, str):
            raw_status = payload.get("status")
        if isinstance(raw_status, str) and raw_status.strip():
            normalized_status = raw_status.strip().lower()
            if normalized_status in {"running", "cancelling", "completed", "failed", "cancelled"}:
                task_status = normalized_status

        stage = payload.get("stage")
        if isinstance(stage, str) and stage.strip():
            self.state.loading_stage = stage.strip()
        elif task_status == "completed":
            self.state.loading_stage = "Completed"
        elif task_status == "failed":
            self.state.loading_stage = "Failed"
        elif task_status == "cancelled":
            self.state.loading_stage = "Cancelled"

        percent = payload.get("percent")
        if isinstance(percent, (int, float)):
            self.state.progress_percent = max(0.0, min(100.0, float(percent)))

        detail = payload.get("detail")
        if isinstance(detail, str):
            self.state.progress_detail = detail.strip()

        if self._active_terminal_task_id:
            await terminal_task_manager.update_progress(
                self._active_terminal_task_id,
                stage=self.state.loading_stage,
                percent=self.state.progress_percent,
                detail=self.state.progress_detail,
                status=task_status,
            )

    async def _run_terminal_action(self, item, user_input: str) -> None:
        requester_name = getattr(getattr(self.command_ctx, "author", None), "display_name", str(self.state.requester_id))
        action_task = asyncio.create_task(
            execute_terminal_action(
                self.bot,
                self.command_ctx,
                item.action,
                user_input,
                access_level=(self.state.access_level or ""),
                progress_callback=self._handle_progress_update,
            )
        )
        task_id = await terminal_task_manager.register_task(
            action_label=item.label,
            requester_id=self.state.requester_id,
            requester_name=str(requester_name),
            task=action_task,
        )
        self._active_terminal_task_id = task_id

        self.state.last_output = f"[{task_id}] Running {item.label}..."
        await self._start_loading(item.label, stage="Running command")

        try:
            result = await action_task
            await terminal_task_manager.mark_completed(task_id)
            self.state.last_output = f"[{task_id}] {result}"
        except asyncio.CancelledError:
            await terminal_task_manager.mark_cancelled(task_id)
            self.state.last_output = f"[{task_id}] {item.label} was cancelled."
        except Exception as exc:
            await terminal_task_manager.mark_failed(task_id, str(exc))
            self.state.last_error = str(exc)
            self.state.last_output = f"[{task_id}] Action failed"
        finally:
            await self._stop_loading()
            if self._active_terminal_task_id == task_id:
                self._active_terminal_task_id = None

    def _format_running_tasks_output(self, tasks) -> str:
        if not tasks:
            return "No terminal commands are currently in progress."

        lines = [
            "IN-PROGRESS TERMINAL COMMANDS",
            "",
        ]

        for task in tasks:
            owner = f"{task.requester_name} ({task.requester_id})"
            stage = task.stage or task.status
            pct = ""
            if task.progress_percent is not None:
                pct = f" {task.progress_percent:.1f}%"

            lines.append(
                f"[{task.task_id}] {task.action_label} | by {owner} | {task.status.upper()} | {stage}{pct} | {task.elapsed_seconds}s"
            )

        return "\n".join(lines)

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

            if item.action == "auth_user":
                self.state.set_access_level("user")
                self.state.last_output = "User terminal access granted."
                await self._redraw(interaction)
                return

            if item.action == "auth_admin":
                modal = TerminalInputModal(title="ADMIN LOGIN", label=item.input_hint or "Admin password")
                await interaction.response.send_modal(modal)
                await modal.wait()
                password = (modal.submitted_value or "").strip()
                admin_password = get_terminal_admin_password()

                if not admin_password:
                    self.state.login_error = "Admin login unavailable: TERMINAL_ADMIN_PASSWORD is not set."
                    await self._redraw_message()
                    return

                if password == admin_password:
                    self.state.set_access_level("admin")
                    self.state.last_output = "Admin terminal access granted."
                    await self._redraw_message()
                    return

                self.state.login_error = "Invalid admin password."
                self.state.last_error = ""
                self.state.last_output = "Admin login failed."
                await self._redraw_message()
                return

            if not self.state.is_authenticated:
                self.state.last_error = "Login required before running commands."
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
                    await self._redraw_message()
                    return

                await self._run_terminal_action(item, user_input)
                await self._redraw_message()
                return

            self.state.last_output = f"Preparing {item.label}..."
            await self._redraw(interaction)

            await self._run_terminal_action(item, user_input)
            await self._redraw_message()
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

    @discord.ui.button(label="View Tasks", style=discord.ButtonStyle.primary, row=1)
    async def view_tasks(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return

        if not self.state.is_authenticated:
            await interaction.response.send_message("Login first to view tasks.", ephemeral=True)
            return

        tasks = await terminal_task_manager.list_running_tasks(
            requester_id=self.state.requester_id,
            is_admin=self.state.is_admin,
        )
        self.state.last_output = self._format_running_tasks_output(tasks)
        self.state.last_error = ""
        await self._redraw(interaction)

    @discord.ui.button(label="Stop Task", style=discord.ButtonStyle.secondary, row=1)
    async def stop_task(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return

        if not self.state.is_authenticated:
            await interaction.response.send_message("Login first to stop tasks.", ephemeral=True)
            return

        modal = TerminalInputModal(title="STOP TASK", label="Task ID")
        await interaction.response.send_modal(modal)
        await modal.wait()

        task_id = (modal.submitted_value or "").strip()
        success, message = await terminal_task_manager.cancel_task(
            task_id=task_id,
            requester_id=self.state.requester_id,
            is_admin=self.state.is_admin,
        )

        self.state.last_output = message
        self.state.last_error = "" if success else message
        await self._redraw_message()

    @discord.ui.button(label="Stop All", style=discord.ButtonStyle.danger, row=1)
    async def stop_all_tasks(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return

        if not self.state.is_authenticated:
            await interaction.response.send_message("Login first to stop tasks.", ephemeral=True)
            return

        _count, message = await terminal_task_manager.cancel_all(
            requester_id=self.state.requester_id,
            is_admin=self.state.is_admin,
        )
        self.state.last_output = message
        self.state.last_error = ""
        await self._redraw(interaction)

    @discord.ui.button(label="Logout", style=discord.ButtonStyle.secondary, row=1)
    async def logout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._authorized(interaction):
            await interaction.response.send_message("Only the requester can use this terminal.", ephemeral=True)
            return

        if not self.state.is_authenticated:
            await interaction.response.send_message("Already at login screen.", ephemeral=True)
            return

        self.state.logout()
        self.state.last_output = "Logged out. Select a terminal mode to continue."
        await self._redraw(interaction)
