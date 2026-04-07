"""Implementation for the #hubs command."""

from datetime import datetime

import discord
from discord.ext import commands


class HubsCommandMixin:
    @commands.command(name="hubs", help="Find highly connected hub players. Usage: #hubs [min_friends]")
    async def find_hubs(self, ctx: commands.Context, min_friends: int = 30):
        """Find players with the most Halo-active connections."""
        try:
            hubs = self.db.find_hubs(min_degree=min_friends, halo_only=True)

            if not hubs:
                await ctx.send(f"No hubs found with {min_friends}+ Halo friends in the database.")
                return

            embed = discord.Embed(
                title=f"Hub Players ({min_friends}+ Halo Friends)",
                description="Players with the most Halo-active connections",
                colour=0x9B59B6,
                timestamp=datetime.now(),
            )

            for i, hub in enumerate(hubs[:10], 1):
                gt = hub.get("gamertag", hub.get("xuid", "Unknown"))
                csr = hub.get("csr") or 0
                kd = hub.get("kd_ratio") or 0

                embed.add_field(
                    name=f"{i}. {gt}",
                    value=f"Friends: **{hub['friend_count']}**\\nCSR: {csr:.0f} | K/D: {kd:.2f}",
                    inline=True,
                )

            embed.set_footer(text=f"Showing top {min(10, len(hubs))} of {len(hubs)} hubs")
            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"Error finding hubs: {str(e)}")
