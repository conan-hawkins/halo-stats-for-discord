"""Implementation for the #graphstats command."""

from datetime import datetime

import discord
from discord.ext import commands


class GraphStatsCommandMixin:
    @commands.command(name="graphstats", help="Show current social graph database totals, depth distribution, and size.")
    async def graph_stats(self, ctx: commands.Context):
        """Display statistics about the social graph database."""
        stats = self.db.get_graph_stats()

        embed = discord.Embed(
            title="Social Graph Statistics",
            description="Current state of the Halo Infinite social network",
            colour=0x00BFFF,
            timestamp=datetime.now(),
        )

        embed.add_field(
            name="Players",
            value=f"Total: **{stats['total_players']:,}**\nHalo Active: **{stats['halo_active_players']:,}**",
            inline=True,
        )

        embed.add_field(
            name="Connections",
            value=(
                f"Friend edges: **{stats['total_friend_edges']:,}**\n"
                f"Co-play edges: **{stats['total_coplay_edges']:,}**"
            ),
            inline=True,
        )

        embed.add_field(
            name="Feature Store",
            value=f"With stats: **{stats['players_with_stats']:,}**",
            inline=True,
        )

        participant_coverage = stats.get("participant_coverage") or {}
        complete_edges = int(participant_coverage.get("complete_edges") or 0)
        partial_edges = int(participant_coverage.get("partial_edges") or 0)
        avg_coverage_ratio = float(participant_coverage.get("avg_coverage_ratio") or 0.0)
        embed.add_field(
            name="Participant Coverage",
            value=(
                f"**{complete_edges:,}** complete | **{partial_edges:,}** partial | "
                f"Avg: **{(avg_coverage_ratio * 100.0):.1f}%**"
            ),
            inline=True,
        )

        embed.add_field(
            name="Graph Metrics",
            value=(
                f"Avg degree: **{stats['avg_friend_degree']:.1f}**\n"
                f"Halo degree: **{stats['avg_halo_friend_degree']:.1f}**"
            ),
            inline=True,
        )

        depth_dist = stats.get("depth_distribution", {})
        if depth_dist:
            depth_str = "\n".join([f"Depth {d}: {c:,}" for d, c in sorted(depth_dist.items())[:5]])
            embed.add_field(
                name="Depth Distribution",
                value=depth_str,
                inline=True,
            )

        embed.add_field(
            name="Database Size",
            value=f"**{stats.get('db_size_mb', 0):.2f}** MB",
            inline=True,
        )

        await ctx.send(embed=embed)
