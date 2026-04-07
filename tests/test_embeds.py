import pytest
from types import SimpleNamespace

from src.bot.embeds import format_error_embed, format_stats_embed
from src.bot.presentation.embed_styles import COLOR_LOADING
from src.bot.presentation.embeds.cache_status import build_cache_status_embed
from src.bot.presentation.embeds.friends import (
    build_xboxfriends_error_embed,
    build_xboxfriends_loading_embed,
    build_xboxfriends_progress_embed,
    build_xboxfriends_result_embed,
)
from src.bot.presentation.embeds.help import build_command_help_embed, build_stats_help_guide_embed
from src.bot.presentation.embeds.loading import build_stats_loading_embed
from src.bot.stats_profiles import STATS_PROFILES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stat_type, expected_label, expected_color",
    [
        ("stats", "OVERALL STATS", 0x00B0F4),
        ("overall", "OVERALL STATS", 0x00B0F4),
        ("ranked", "RANKED STATS", 0xFFD700),
        ("social", "CASUAL STATS", 0x00FF00),
        ("unknown", "OVERALL STATS", 0x00B0F4),
    ],
)
async def test_format_stats_embed_uses_profile_mapping(stat_type, expected_label, expected_color):
    stats_list = ["1.5", "50.0%", "2.0", "10", "15", "5", "20"]

    embed = await format_stats_embed("PlayerOne", stats_list, stat_type=stat_type)

    assert embed.title == f"PLAYERONE - {expected_label}"
    assert embed.colour.value == expected_color


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_no, expected_title",
    [
        (1, "ERROR - USE OF UNAUTHORISED CHARACTERS DETECTED."),
        (2, "ERROR - PLAYER NOT FOUND. PLEASE CHECK SPELLING."),
        (3, "ERROR - PLAYERS PROFILE IS SET TO PRIVATE."),
        (4, "ERROR - SOMETHING UNEXPECTED HAPPENED."),
        (999, "ERROR - UNKNOWN ERROR OCCURRED."),
    ],
)
async def test_format_error_embed_maps_titles(error_no, expected_title):
    embed = await format_error_embed(error_no)

    assert embed.title == expected_title
    assert embed.colour.value == 0xFF0000


def test_build_stats_loading_embed_formats_all_matches():
    embed = build_stats_loading_embed("PlayerOne")

    assert embed.title == "Loading Stats..."
    assert "from ALL matches" in (embed.description or "")
    assert embed.colour.value == COLOR_LOADING


def test_build_stats_loading_embed_formats_limited_matches():
    embed = build_stats_loading_embed("PlayerOne", matches_to_process=25)

    assert "from 25 matches" in (embed.description or "")


def test_build_command_help_embed_formats_usage_and_tip_footer():
    cmd = SimpleNamespace(name="ranked", signature="<gamertag>", help="Ranked help text")

    embed = build_command_help_embed(cmd)

    assert embed.title == "Help: #ranked"
    assert embed.fields[0].name == "Usage"
    assert "#ranked <gamertag>" in embed.fields[0].value
    assert "Gamertags with spaces" in (embed.footer.text or "")


def test_build_stats_help_guide_embed_lists_profile_commands():
    embed = build_stats_help_guide_embed(STATS_PROFILES)

    assert embed.title == "Halo Bot Command Guide"
    stats_field = next(field for field in embed.fields if field.name == "Player Stats Commands")
    assert "#full <gamertag>" in stats_field.value
    assert "#ranked <gamertag>" in stats_field.value
    assert "#casual <gamertag>" in stats_field.value


def test_build_cache_status_embed_formats_metrics_for_active_progress():
    metrics = SimpleNamespace(
        xuid_mappings=2,
        processed_matches=25,
        total_matches=50,
        resolved_gamertags=4,
        progress_state="ok",
    )

    embed = build_cache_status_embed(metrics)
    values = [field.value for field in embed.fields]

    assert any("Total mappings: **2**" in value for value in values)
    assert any("Processed: **25** / **50** matches" in value for value in values)
    assert any("Resolved gamertags: **4**" in value for value in values)


def test_build_cache_status_embed_formats_missing_progress_state():
    metrics = SimpleNamespace(
        xuid_mappings=3,
        processed_matches=0,
        total_matches=0,
        resolved_gamertags=3,
        progress_state="missing",
    )

    embed = build_cache_status_embed(metrics)
    progress_field = next(field for field in embed.fields if field.name == "Match Scan Progress")

    assert "No active match scan progress file" in progress_field.value


def test_build_xboxfriends_loading_embed_formats_gamertag():
    embed = build_xboxfriends_loading_embed("PlayerOne")

    assert embed.title == "🔍 Fetching Friends List..."
    assert "**PlayerOne**" in (embed.description or "")
    assert embed.colour.value == COLOR_LOADING


def test_build_xboxfriends_progress_embed_formats_friends_found_stage():
    embed = build_xboxfriends_progress_embed("PlayerOne", current=0, total=7, stage="friends_found", fof_count=0)

    assert embed.title == "🔍 Fetching Friends of Friends..."
    assert "Found **7** direct friends" in (embed.description or "")
    assert "Progress: 0/7 friends checked" in (embed.description or "")


def test_build_xboxfriends_progress_embed_formats_active_progress_stage():
    embed = build_xboxfriends_progress_embed("PlayerOne", current=3, total=10, stage="checking_fof", fof_count=9)

    description = embed.description or ""
    assert "Progress: **3/10** friends checked" in description
    assert "30%" in description
    assert "Found **9** unique 2nd-degree connections so far" in description


def test_build_xboxfriends_error_embed_formats_description():
    embed = build_xboxfriends_error_embed("request failed")

    assert embed.title == "❌ Error"
    assert embed.description == "request failed"
    assert embed.colour.value == 0xFF0000


def test_build_xboxfriends_result_embed_formats_blacklist_sections_and_summary():
    friends = [
        {"xuid": "friend-1", "gamertag": "Trusted"},
        {"xuid": "friend-2", "gamertag": "BlockedDirect"},
    ]
    friends_of_friends = [
        {"xuid": "fof-1", "gamertag": "BlockedFoF", "via": "Trusted"},
        {"xuid": "fof-1", "gamertag": "BlockedFoF", "via": "BlockedDirect"},
    ]
    private_friends = [{"xuid": "private-1", "gamertag": "PrivateOne"}]
    blacklist = {
        "friend-2": "Blocked Direct",
        "fof-1": "Blocked FoF",
        "private-1": "Blocked Private",
    }

    embed = build_xboxfriends_result_embed(
        gamertag="SeedPlayer",
        friends=friends,
        friends_of_friends=friends_of_friends,
        private_friends=private_friends,
        blacklist=blacklist,
    )

    assert embed.title == "👥 Friends Network: SeedPlayer"

    direct_field = embed.fields[0]
    assert direct_field.name == "📋 Direct Friends (2)"
    assert "• Blocked Direct" in direct_field.value
    assert "Private Friends List (1):" in direct_field.value
    assert "• PrivateOne" in direct_field.value

    fof_field = embed.fields[1]
    assert fof_field.name == "🔗 Friends of Friends (2)"
    assert "• Blocked FoF x2" in fof_field.value

    summary_field = embed.fields[2]
    assert summary_field.name == "📊 Summary"
    assert "**Direct friends:** 2" in summary_field.value
    assert "**Blacklisted friends:** 1" in summary_field.value
    assert "**Private friends lists:** 1" in summary_field.value
    assert "**2nd degree friends:** 2" in summary_field.value
    assert "**Blacklisted 2nd degree friends:** 2" in summary_field.value
    assert "Private-list blacklist hits" not in summary_field.value
