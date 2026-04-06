import asyncio
import io
from datetime import datetime
from typing import Optional, Tuple

import discord

from .state import TerminalState


CANVAS_WIDTH = 1360
CANVAS_HEIGHT = 900
SCANLINE_STEP = 3
SCANLINE_ALPHA = 44
GLOW_STRENGTH_OFFSETS = [(-1, 0), (1, 0), (0, -1), (0, 1), (-2, 0), (2, 0)]
SCREEN_LABELS = {
    "login": "LOGIN",
    "root": "MAIN MENU",
    "status": "DATABASE STATUS",
    "stats": "STATS",
    "social": "SOCIAL",
    "iss": "ISS",
    "crawl": "CRAWL",
}
LOADING_FRAMES = ["[=     ]", "[==    ]", "[===   ]", "[ ===  ]", "[  === ]", "[   ===]", "[    ==]", "[     =]"]


def _build_lines(state: TerminalState) -> str:
    menu = state.current_menu()
    active_screen_key = state.menu_key if state.is_authenticated else "login"
    screen_name = SCREEN_LABELS.get(active_screen_key, active_screen_key.upper())
    lines = []
    lines.append("PROJECT GOLIATH // ISS TERMINAL")
    lines.append(f"TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if state.is_authenticated:
        lines.append(f"ACCESS: {state.access_level.upper()}")
    else:
        lines.append("ACCESS: LOCKED")
    lines.append(f"SCREEN: {screen_name}")
    lines.append("")

    if not state.is_authenticated:
        lines.append("LOGIN:")
        lines.append("Select a terminal access level:")
        lines.append("")
        for index, item in enumerate(menu):
            marker = ">" if index == state.selected_index else " "
            lines.append(f"{marker} {item.label}")
        if state.login_error:
            lines.append("")
            lines.append("ERROR:")
            lines.append(state.login_error[-300:])

        return "\n".join(lines)

    lines.append("MENU:")

    for index, item in enumerate(menu):
        marker = ">" if index == state.selected_index else " "
        lines.append(f"{marker} {item.label}")

    lines.append("")
    if state.is_loading:
        frame = LOADING_FRAMES[state.loading_tick % len(LOADING_FRAMES)]
        elapsed = "0s"
        if state.loading_started_at is not None:
            elapsed = f"{max(0, int((datetime.now() - state.loading_started_at).total_seconds()))}s"
        lines.append("LOADING:")
        lines.append(f"{frame} {state.loading_label}".strip())
        if state.loading_stage:
            lines.append(f"Stage: {state.loading_stage}")
        if state.progress_percent is not None:
            pct = max(0.0, min(100.0, float(state.progress_percent)))
            segments = 20
            filled = int(round((pct / 100.0) * segments))
            bar = "#" * filled + "-" * (segments - filled)
            lines.append(f"Progress: [{bar}] {pct:.1f}%")
        if state.progress_detail:
            lines.append(f"Detail: {state.progress_detail}")
        lines.append(f"Elapsed: {elapsed}")
        lines.append("")

    lines.append("OUTPUT:")
    lines.append(state.last_output[-600:] if state.last_output else "")
    if state.last_error:
        lines.append("")
        lines.append("ERROR:")
        lines.append(state.last_error[-300:])

    return "\n".join(lines)


def _build_menu_help_text(state: TerminalState) -> str:
    menu = state.current_menu()
    lines = ["**Menu Buttons**"]

    for index, item in enumerate(menu):
        marker = ">" if index == state.selected_index else "-"
        detail = item.description or "Run this action."
        if item.submenu:
            detail = item.description or "Open this submenu."
        input_hint = f" Input: {item.input_hint}." if item.requires_input and item.input_hint else ""
        lines.append(f"`{marker} {item.label}` {detail}{input_hint}")

    return "\n".join(lines)


def _render_terminal_png(text: str) -> Optional[bytes]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return None

    width, height = CANVAS_WIDTH, CANVAS_HEIGHT
    bg = (7, 14, 7)
    fg = (108, 255, 112)
    glow = (30, 140, 38)

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("consola.ttf", 32)
    except Exception:
        font = ImageFont.load_default()

    margin_x = 40
    margin_y = 34
    line_height = 42

    # Subtle top-to-bottom phosphor gradient so the frame does not look flat.
    for y in range(height):
        shade = int(7 + ((height - y) / height) * 7)
        draw.line((0, y, width, y), fill=(shade, shade + 7, shade))

    # Scanline overlay for CRT feel without expensive blur filters.
    scanline_dark = max(0, 22 - (SCANLINE_ALPHA // 6))
    for y in range(0, height, SCANLINE_STEP):
        draw.line((0, y, width, y), fill=(0, scanline_dark, 0), width=1)

    y = margin_y
    for line in text.splitlines():
        for x_off, y_off in GLOW_STRENGTH_OFFSETS:
            draw.text((margin_x + x_off, y + y_off), line, fill=glow, font=font)
        draw.text((margin_x, y), line, fill=fg, font=font)
        y += line_height
        if y > height - line_height:
            break

    # Vignette + border gives the display a framed terminal panel look.
    draw.rectangle([8, 8, width - 8, height - 8], outline=(52, 168, 58), width=2)
    draw.rectangle([16, 16, width - 16, height - 16], outline=(30, 95, 35), width=1)
    draw.rectangle([0, 0, width, 24], fill=(0, 0, 0))
    draw.rectangle([0, height - 24, width, height], fill=(0, 0, 0))

    out = io.BytesIO()
    img.save(out, format="PNG")
    return out.getvalue()


def build_terminal_message_payload(state: TerminalState) -> Tuple[discord.Embed, Optional[discord.File]]:
    text = _build_lines(state)
    png_bytes = _render_terminal_png(text)
    menu_help = _build_menu_help_text(state)

    embed = discord.Embed(
        title="TERMINAL CONTROL",
        colour=0x2FB33A,
        timestamp=datetime.now(),
    )

    if png_bytes:
        file = discord.File(io.BytesIO(png_bytes), filename="terminal_screen.png")
        embed.set_image(url="attachment://terminal_screen.png")
        embed.description = menu_help
        return embed, file

    # Fallback text mode if Pillow is unavailable.
    embed.description = f"{menu_help}\n\n```ansi\n{text}\n```"
    return embed, None


async def build_terminal_message_payload_async(state: TerminalState) -> Tuple[discord.Embed, Optional[discord.File]]:
    """Render message payload without blocking the event loop on image generation."""
    text = _build_lines(state)
    png_bytes = await asyncio.to_thread(_render_terminal_png, text)

    embed = discord.Embed(
        title="TERMINAL CONTROL",
        colour=0x2FB33A,
        timestamp=datetime.now(),
    )

    menu_help = _build_menu_help_text(state)

    if png_bytes:
        file = discord.File(io.BytesIO(png_bytes), filename="terminal_screen.png")
        embed.set_image(url="attachment://terminal_screen.png")
        embed.description = menu_help
        return embed, file

    embed.description = f"{menu_help}\n\n```ansi\n{text}\n```"
    return embed, None
