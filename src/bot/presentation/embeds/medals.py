"""Composite PNG grid of a player's earned medals, wrapped in a Discord embed."""

import asyncio
import io
from typing import Dict, List, Optional, Tuple

import discord

from src.api.medal_icons import get_medal_icon_bytes
from src.bot.presentation.embed_styles import apply_footer, current_timestamp
from src.bot.stats_profiles import get_stats_profile_for_fetch_type

GRID_COLUMNS = 5
MAX_DISPLAYED_MEDALS = 40

# Curated subset shown on the picture: the personal kill-streak chain, the
# full multi-kill chain, and Perfect/Perfection - not every earned medal.
# IDs from src/database/schema.py's MEDAL_NAME_MAPPING.
_STREAK_MEDAL_IDS = {
    2780740615,  # Killing Spree
    4261842076,  # Killing Frenzy
    418532952,   # Running Riot
    1486797009,  # Rampage
    710323196,   # Nightmare
    1720896992,  # Boogeyman
    2567026752,  # Grim Reaper
    2875941471,  # Demon
}
_MULTI_KILL_MEDAL_IDS = {
    622331684,   # Double Kill
    2063152177,  # Triple Kill
    835814121,   # Overkill
    2137071619,  # Killtacular
    1430343434,  # Killtrocity
    3835606176,  # Killamanjaro
    2242633421,  # Killtastrophe
    3352648716,  # Killpocalypse
    3233051772,  # Killionaire
}
_PERFECT_MEDAL_IDS = {
    1512363953,  # Perfect
    865763896,   # Perfection
}
SHOWN_MEDAL_IDS = _STREAK_MEDAL_IDS | _MULTI_KILL_MEDAL_IDS | _PERFECT_MEDAL_IDS

# One picture row per category, in this order; a category with nothing
# earned is skipped entirely rather than rendered as an empty row.
_ROW_GROUPS: Tuple[Tuple[str, str], ...] = (
    ("streak", "STREAK MEDALS"),
    ("multi_kill", "MULTI-KILL MEDALS"),
    ("perfect", "PERFECT"),
)

CELL_WIDTH = 260
CELL_HEIGHT = 96
CELL_MARGIN = 16
ICON_PX = 64
LABEL_HEIGHT = 24

BACKGROUND_COLOR = (24, 24, 28)
CELL_TEXT_COLOR = (235, 235, 235)
COUNT_COLOR = (255, 209, 92)
PLACEHOLDER_COLOR = (60, 60, 68)
LABEL_COLOR = (160, 160, 168)


def filter_shown_medals(earned_medals: List[Dict]) -> List[Dict]:
    """Narrow an earned-medals list down to the curated streak/multi-kill/Perfect
    subset shown on the picture, preserving the existing count-desc ordering."""
    return [medal for medal in earned_medals if medal["medal_id"] in SHOWN_MEDAL_IDS]


def _medal_row_category(medal_id: int) -> Optional[str]:
    if medal_id in _STREAK_MEDAL_IDS:
        return "streak"
    if medal_id in _MULTI_KILL_MEDAL_IDS:
        return "multi_kill"
    if medal_id in _PERFECT_MEDAL_IDS:
        return "perfect"
    return None


def _group_medals_by_row(earned_medals: List[Dict]) -> List[Tuple[str, List[Dict]]]:
    """Bucket medals into (label, medals) rows in _ROW_GROUPS order, dropping
    empty categories. earned_medals is already sorted by count desc (see
    get_player_earned_medals), and partitioning preserves that order, so each
    bucket ends up "most earned first" with no extra sort needed."""
    buckets: Dict[str, List[Dict]] = {key: [] for key, _label in _ROW_GROUPS}
    for medal in earned_medals:
        category = _medal_row_category(medal["medal_id"])
        if category:
            buckets[category].append(medal)
    return [(label, buckets[key]) for key, label in _ROW_GROUPS if buckets[key]]


async def fetch_medal_icons(medal_ids: List[int]) -> Dict[int, Optional[bytes]]:
    """Fetch icon bytes for each medal id, tolerating individual failures."""
    results = await asyncio.gather(
        *(get_medal_icon_bytes(medal_id) for medal_id in medal_ids),
        return_exceptions=True,
    )
    icon_bytes_by_id: Dict[int, Optional[bytes]] = {}
    for medal_id, result in zip(medal_ids, results):
        icon_bytes_by_id[medal_id] = result if isinstance(result, (bytes, bytearray)) else None
    return icon_bytes_by_id


def _load_fonts():
    from PIL import ImageFont

    try:
        name_font = ImageFont.truetype("consola.ttf", 15)
        count_font = ImageFont.truetype("consolab.ttf", 20)
        label_font = ImageFont.truetype("consolab.ttf", 16)
    except Exception:
        name_font = ImageFont.load_default()
        count_font = ImageFont.load_default()
        label_font = ImageFont.load_default()
    return name_font, count_font, label_font


def _wrap_text(draw, text: str, font, max_width: int, max_lines: int = 2) -> List[str]:
    words = text.split()
    lines: List[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if not current or draw.textlength(candidate, font=font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
            if len(lines) == max_lines - 1:
                break
    if current:
        lines.append(current)

    if len(lines) > max_lines:
        lines = lines[:max_lines]
    if len(lines) == max_lines:
        last = lines[-1]
        while last and draw.textlength(last + "...", font=font) > max_width:
            last = last[:-1]
        if last != lines[-1]:
            lines[-1] = last.rstrip() + "..."
    return lines


def _draw_centered(draw, box_xy, text: str, font, fill) -> None:
    x, y, w, h = box_xy
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(
        (x + (w - text_w) / 2 - bbox[0], y + (h - text_h) / 2 - bbox[1]),
        text,
        font=font,
        fill=fill,
    )


def _draw_medal_cell(Image, draw, img, cell_x: int, cell_y: int, medal: Dict, icon_bytes_by_id, name_font, count_font) -> None:
    icon_x = cell_x
    icon_y = cell_y + (CELL_HEIGHT - ICON_PX) // 2

    icon_bytes = icon_bytes_by_id.get(medal["medal_id"])
    pasted = False
    if icon_bytes:
        try:
            with Image.open(io.BytesIO(icon_bytes)) as icon:
                icon_rgba = icon.convert("RGBA")
                img.paste(icon_rgba, (icon_x, icon_y), icon_rgba)
                pasted = True
        except Exception:
            pasted = False

    if not pasted:
        draw.rounded_rectangle(
            (icon_x, icon_y, icon_x + ICON_PX, icon_y + ICON_PX),
            radius=10,
            fill=PLACEHOLDER_COLOR,
        )
        initial = (medal["medal_name"] or "?")[:1].upper()
        _draw_centered(draw, (icon_x, icon_y, ICON_PX, ICON_PX), initial, count_font, CELL_TEXT_COLOR)

    text_x = icon_x + ICON_PX + 14
    text_max_width = CELL_WIDTH - ICON_PX - 14
    name_lines = _wrap_text(draw, medal["medal_name"], name_font, text_max_width)
    text_y = cell_y + 8
    for line in name_lines:
        draw.text((text_x, text_y), line, font=name_font, fill=CELL_TEXT_COLOR)
        text_y += 18

    draw.text((text_x, cell_y + CELL_HEIGHT - 58), f"x{medal['count']}", font=count_font, fill=COUNT_COLOR)


def render_medals_grid_png(earned_medals: List[Dict], icon_bytes_by_id: Dict[int, Optional[bytes]]) -> bytes:
    """Composite a grid image, one row per medal category (streak, multi-kill,
    Perfect/Perfection), each ordered most-earned-first left to right."""
    from PIL import Image, ImageDraw

    name_font, count_font, label_font = _load_fonts()

    row_groups = _group_medals_by_row(earned_medals[:MAX_DISPLAYED_MEDALS])

    def _rows_for(medals: List[Dict]) -> int:
        return max(1, -(-len(medals) // GRID_COLUMNS))  # ceil div

    width = CELL_MARGIN + GRID_COLUMNS * (CELL_WIDTH + CELL_MARGIN)
    height = CELL_MARGIN + sum(
        LABEL_HEIGHT + _rows_for(medals) * (CELL_HEIGHT + CELL_MARGIN) for _label, medals in row_groups
    )
    if not row_groups:
        height = CELL_MARGIN + CELL_HEIGHT + CELL_MARGIN

    img = Image.new("RGB", (width, height), BACKGROUND_COLOR)
    draw = ImageDraw.Draw(img)

    y_cursor = CELL_MARGIN
    for label, medals in row_groups:
        draw.text((CELL_MARGIN, y_cursor), label, font=label_font, fill=LABEL_COLOR)
        y_cursor += LABEL_HEIGHT

        for index, medal in enumerate(medals):
            col = index % GRID_COLUMNS
            row = index // GRID_COLUMNS
            cell_x = CELL_MARGIN + col * (CELL_WIDTH + CELL_MARGIN)
            cell_y = y_cursor + row * (CELL_HEIGHT + CELL_MARGIN)
            _draw_medal_cell(Image, draw, img, cell_x, cell_y, medal, icon_bytes_by_id, name_font, count_font)

        y_cursor += _rows_for(medals) * (CELL_HEIGHT + CELL_MARGIN)

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()


async def build_medals_embed(
    gamertag: str,
    stat_type: str,
    earned_medals: List[Dict],
    png_bytes: Optional[bytes],
) -> Tuple[discord.Embed, Optional[discord.File]]:
    """Wrap a rendered medals grid (or an empty-state message) in a Discord embed."""
    profile = get_stats_profile_for_fetch_type(stat_type)

    embed = discord.Embed(
        title=f"{gamertag.upper()} - MEDALS ({profile.display_name})",
        colour=profile.embed_color,
        timestamp=current_timestamp(),
    )

    if not earned_medals:
        embed.description = "No streak, multi-kill, or Perfect/Perfection medals earned in this mode yet."
        apply_footer(embed)
        return embed, None

    total_shown = min(len(earned_medals), MAX_DISPLAYED_MEDALS)
    description = f"{total_shown} key medal type{'s' if total_shown != 1 else ''} earned"
    if len(earned_medals) > MAX_DISPLAYED_MEDALS:
        description += f" (+{len(earned_medals) - MAX_DISPLAYED_MEDALS} more not shown)"
    embed.description = description

    file = None
    if png_bytes:
        file = discord.File(io.BytesIO(png_bytes), filename="medals.png")
        embed.set_image(url="attachment://medals.png")

    apply_footer(embed)
    return embed, file
