"""'Show Medals' button attached to the #full/#ranked/#casual stats embeds."""

from typing import Optional

import discord

from src.api.client import api_client
from src.bot.presentation.embeds.medals import (
    MAX_DISPLAYED_MEDALS,
    build_medals_embed,
    fetch_medal_icons,
    filter_shown_medals,
    render_medals_grid_png,
)

SHOW_MEDALS_TIMEOUT_SECONDS = 300


class ShowMedalsView(discord.ui.View):
    """Open to all users - this button only re-displays already-public player
    data and mutates no shared state, unlike the terminal/graph views that
    gate personalized sessions behind a requester check."""

    def __init__(self, gamertag: str, xuid: str, stat_type: str, requester_id: int):
        super().__init__(timeout=SHOW_MEDALS_TIMEOUT_SECONDS)
        self.gamertag = gamertag
        self.xuid = xuid
        self.stat_type = stat_type
        self.requester_id = requester_id
        self.message: Optional[discord.Message] = None
        self._fetch_in_progress = False

    async def on_timeout(self):
        if not self.message:
            return
        for item in self.children:
            item.disabled = True
        try:
            await self.message.edit(view=self)
        except Exception:
            return

    @discord.ui.button(label="Show Medals", emoji="\U0001F396", style=discord.ButtonStyle.primary)
    async def show_medals(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._fetch_in_progress:
            await interaction.response.send_message(
                "Already fetching medals, hang on...", ephemeral=True
            )
            return

        self._fetch_in_progress = True
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=False)

            earned_medals = api_client.stats_cache.get_player_earned_medals(self.xuid, self.stat_type)
            shown_medals = filter_shown_medals(earned_medals)

            png_bytes = None
            if shown_medals:
                medal_ids = [medal["medal_id"] for medal in shown_medals[:MAX_DISPLAYED_MEDALS]]
                icon_bytes_by_id = await fetch_medal_icons(medal_ids)
                png_bytes = render_medals_grid_png(shown_medals, icon_bytes_by_id)

            embed, file = await build_medals_embed(self.gamertag, self.stat_type, shown_medals, png_bytes)

            # One-shot: remove the button entirely so a second click can't
            # append a duplicate medals embed to the same message.
            self.remove_item(button)
            existing_embeds = list(interaction.message.embeds)
            edit_kwargs = {"embeds": [*existing_embeds, embed], "view": self}
            if file:
                edit_kwargs["attachments"] = [file]
            await interaction.edit_original_response(**edit_kwargs)
            self.stop()
        except Exception as e:
            print(f"[stats_medals_view] Error building medals display: {e}")
            try:
                await interaction.followup.send(
                    "Something went wrong building the medals display. Try again shortly.",
                    ephemeral=True,
                )
            except Exception:
                pass
        finally:
            self._fetch_in_progress = False


__all__ = ["ShowMedalsView"]
