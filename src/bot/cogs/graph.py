"""
Graph Commands Cog for Halo Stats Discord Bot

Contains commands for social graph analysis:
- #graphstats - Show current graph statistics
- #similar - Find similar players (KNN)
- #hubs - Find hub players with many connections
- #network - Show a player's Halo-active friend network
- #crawl - Start background crawl from a seed player
"""

import asyncio
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands

from src.database.graph_schema import get_graph_db
from src.api import api_client


class GraphCog(commands.Cog, name="Graph"):
    """Commands for social graph analysis and network discovery"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()
        self._crawl_task: Optional[asyncio.Task] = None
    
    @commands.command(name='graphstats', help='Show current social graph statistics')
    async def graph_stats(self, ctx: commands.Context):
        """Display statistics about the social graph database"""
        stats = self.db.get_graph_stats()
        
        embed = discord.Embed(
            title="Social Graph Statistics",
            description="Current state of the Halo Infinite social network",
            colour=0x00BFFF,
            timestamp=datetime.now()
        )
        
        # Node counts
        embed.add_field(
            name="Players",
            value=f"Total: **{stats['total_players']:,}**\nHalo Active: **{stats['halo_active_players']:,}**",
            inline=True
        )
        
        # Edge counts
        embed.add_field(
            name="Connections",
            value=f"Friend edges: **{stats['total_friend_edges']:,}**\nCo-play edges: **{stats['total_coplay_edges']:,}**",
            inline=True
        )
        
        # Stats coverage
        embed.add_field(
            name="Feature Store",
            value=f"With stats: **{stats['players_with_stats']:,}**",
            inline=True
        )
        
        # Graph structure
        embed.add_field(
            name="Graph Metrics",
            value=f"Avg degree: **{stats['avg_friend_degree']:.1f}**\nHalo degree: **{stats['avg_halo_friend_degree']:.1f}**",
            inline=True
        )
        
        # Depth distribution
        depth_dist = stats.get('depth_distribution', {})
        if depth_dist:
            depth_str = "\n".join([f"Depth {d}: {c:,}" for d, c in sorted(depth_dist.items())[:5]])
            embed.add_field(
                name="Depth Distribution",
                value=depth_str,
                inline=True
            )
        
        # Database size
        embed.add_field(
            name="Database Size",
            value=f"**{stats.get('db_size_mb', 0):.2f}** MB",
            inline=True
        )
        
        await ctx.send(embed=embed)
    
    @commands.command(name='similar', help='Find similar Halo players. Example - #similar XxUK D3STROYxX')
    async def find_similar(self, ctx: commands.Context, *inputs):
        """Find players with similar stats using KNN"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#similar GAMERTAG`")
            return
        
        gamertag = ' '.join(inputs)
        
        # Loading message
        loading_embed = discord.Embed(
            title="Finding Similar Players...",
            description=f"Searching for players similar to **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now()
        )
        loading_msg = await ctx.send(embed=loading_embed)
        
        try:
            # Resolve gamertag to XUID
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.edit(embed=discord.Embed(
                    title="Player Not Found",
                    description=f"Could not find player **{gamertag}**",
                    colour=0xFF0000
                ))
                return
            
            # Check if player is in graph
            player = self.db.get_player(xuid)
            features = self.db.get_halo_features(xuid)
            
            if not features or features.get('matches_played', 0) == 0:
                await loading_msg.edit(embed=discord.Embed(
                    title="No Stats Available",
                    description=f"**{gamertag}** is not in the graph database.\nRun a crawl that includes this player first.",
                    colour=0xFF0000
                ))
                return
            
            # Find similar players
            similar = self.db.get_similar_players_knn(xuid, k=5)
            
            if not similar:
                await loading_msg.edit(embed=discord.Embed(
                    title="No Similar Players Found",
                    description="Not enough players in database to find matches.",
                    colour=0xFF0000
                ))
                return
            
            # Build response
            embed = discord.Embed(
                title=f"Players Similar to {gamertag}",
                description=f"Based on CSR, K/D, and Win Rate",
                colour=0x00FF00,
                timestamp=datetime.now()
            )
            
            # Target player stats
            embed.add_field(
                name=f"Target: {gamertag}",
                value=f"CSR: {features.get('csr', 0):.0f} | K/D: {features.get('kd_ratio', 0):.2f} | Win: {features.get('win_rate', 0):.1f}%",
                inline=False
            )
            
            # Similar players
            for i, p in enumerate(similar, 1):
                gt = p.get('gamertag', p.get('xuid', 'Unknown'))
                embed.add_field(
                    name=f"{i}. {gt}",
                    value=f"CSR: {p.get('csr', 0):.0f} | K/D: {p.get('kd_ratio', 0):.2f} | Win: {p.get('win_rate', 0):.1f}%\nMatches: {p.get('matches_played', 0)}",
                    inline=True
                )
            
            await loading_msg.edit(embed=embed)
            
        except Exception as e:
            await loading_msg.edit(embed=discord.Embed(
                title="Error",
                description=f"Error finding similar players: {str(e)}",
                colour=0xFF0000
            ))
    
    @commands.command(name='hubs', help='Find hub players with many Halo friends')
    async def find_hubs(self, ctx: commands.Context, min_friends: int = 30):
        """Find players with the most Halo-active connections"""
        
        try:
            hubs = self.db.find_hubs(min_degree=min_friends, halo_only=True)
            
            if not hubs:
                await ctx.send(f"No hubs found with {min_friends}+ Halo friends in the database.")
                return
            
            embed = discord.Embed(
                title=f"Hub Players ({min_friends}+ Halo Friends)",
                description="Players with the most Halo-active connections",
                colour=0x9B59B6,
                timestamp=datetime.now()
            )
            
            for i, hub in enumerate(hubs[:10], 1):
                gt = hub.get('gamertag', hub.get('xuid', 'Unknown'))
                csr = hub.get('csr', 0)
                kd = hub.get('kd_ratio', 0)
                
                embed.add_field(
                    name=f"{i}. {gt}",
                    value=f"Friends: **{hub['friend_count']}**\nCSR: {csr:.0f} | K/D: {kd:.2f}",
                    inline=True
                )
            
            embed.set_footer(text=f"Showing top {min(10, len(hubs))} of {len(hubs)} hubs")
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error finding hubs: {str(e)}")
    
    @commands.command(name='network', help='Show a player\'s Halo friend network. Example - #network XxUK D3STROYxX')
    async def show_network(self, ctx: commands.Context, *inputs):
        """Show a player's Halo-active friend network"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#network GAMERTAG`")
            return
        
        gamertag = ' '.join(inputs)
        
        try:
            # Resolve gamertag
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await ctx.send(f"Could not find player **{gamertag}**")
                return
            
            # Get player info
            player = self.db.get_player(xuid)
            if not player:
                await ctx.send(f"**{gamertag}** is not in the graph database. Run a crawl first.")
                return
            
            # Get Halo friends
            halo_friends = self.db.get_halo_friends(xuid)
            all_friends = self.db.get_friends(xuid)
            
            embed = discord.Embed(
                title=f"Network: {gamertag}",
                colour=0x3498DB,
                timestamp=datetime.now()
            )
            
            # Summary
            embed.add_field(
                name="Summary",
                value=f"Total friends: **{len(all_friends)}**\nHalo friends: **{len(halo_friends)}**\nCrawl depth: **{player.get('crawl_depth', '?')}**",
                inline=True
            )
            
            # Top Halo friends by CSR
            if halo_friends:
                sorted_friends = sorted(
                    halo_friends, 
                    key=lambda x: x.get('csr') or 0, 
                    reverse=True
                )
                
                top_str = ""
                for f in sorted_friends[:5]:
                    gt = f.get('gamertag', f.get('dst_xuid', 'Unknown'))
                    csr = f.get('csr', 0)
                    kd = f.get('kd_ratio', 0)
                    top_str += f"**{gt}**: CSR {csr:.0f}, K/D {kd:.2f}\n"
                
                embed.add_field(
                    name="Top Halo Friends (by CSR)",
                    value=top_str or "None",
                    inline=False
                )
            
            # Get player's own stats if available
            features = self.db.get_halo_features(xuid)
            if features and features.get('matches_played', 0) > 0:
                embed.add_field(
                    name="Player Stats",
                    value=f"CSR: {features.get('csr', 0):.0f}\nK/D: {features.get('kd_ratio', 0):.2f}\nWin Rate: {features.get('win_rate', 0):.1f}%\nMatches: {features.get('matches_played', 0)}",
                    inline=True
                )
            
            embed.set_footer(text=f"XUID: {xuid}")
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error showing network: {str(e)}")
    
    @commands.command(name='crawl', help='Start a background crawl from a seed player. Admin only.')
    @commands.has_permissions(administrator=True)
    async def start_crawl(self, ctx: commands.Context, *inputs):
        """Start a background crawl (admin only)"""
        if not inputs:
            await ctx.send("Usage: `#crawl GAMERTAG [depth]`\nExample: `#crawl YourGamertag 2`\nNote: Wrap gamertags with spaces in quotes: `#crawl \"Possibly Tom\" 2`")
            return
        
        # Handle gamertags with spaces: check if last arg is a number (depth)
        if len(inputs) > 1 and inputs[-1].isdigit():
            # Last arg is depth, everything else is the gamertag
            gamertag = ' '.join(inputs[:-1])
            depth = int(inputs[-1])
        else:
            # No depth specified, entire input is the gamertag
            gamertag = ' '.join(inputs)
            depth = 2
        
        if self._crawl_task and not self._crawl_task.done():
            await ctx.send("A crawl is already running. Wait for it to complete or restart the bot.")
            return
        
        await ctx.send(f"Starting background crawl from **{gamertag}** with depth {depth}...\nUse `#graphstats` to check progress.")
        
        # Import here to avoid circular imports
        from src.graph.crawler import GraphCrawler, CrawlConfig
        
        async def run_crawl():
            try:
                config = CrawlConfig(
                    max_depth=depth,
                    collect_stats=True,
                    stats_matches_to_process=25
                )
                crawler = GraphCrawler(api_client, config, self.db)
                progress = await crawler.crawl_from_seed(seed_gamertag=gamertag)
                
                # Send completion message
                channel = ctx.channel
                embed = discord.Embed(
                    title="Crawl Complete",
                    colour=0x00FF00,
                    timestamp=datetime.now()
                )
                embed.add_field(name="Seed", value=gamertag, inline=True)
                embed.add_field(name="Depth", value=str(depth), inline=True)
                embed.add_field(name="Nodes Discovered", value=str(progress.nodes_discovered), inline=True)
                embed.add_field(name="Halo Players", value=str(progress.halo_players_found), inline=True)
                embed.add_field(name="Edges", value=str(progress.edges_discovered), inline=True)
                embed.add_field(name="With Stats", value=str(progress.nodes_with_stats), inline=True)
                
                await channel.send(embed=embed)
                
            except Exception as e:
                await ctx.channel.send(f"Crawl error: {str(e)}")
        
        self._crawl_task = asyncio.create_task(run_crawl())
    
    @commands.command(name='crawlstop', help='Stop the current background crawl. Admin only.')
    @commands.has_permissions(administrator=True)
    async def stop_crawl(self, ctx: commands.Context):
        """Stop the current background crawl"""
        if self._crawl_task and not self._crawl_task.done():
            self._crawl_task.cancel()
            await ctx.send("Crawl task cancelled. Progress has been saved.")
        else:
            await ctx.send("No crawl is currently running.")


async def setup(bot: commands.Bot):
    """Setup function for loading the cog"""
    await bot.add_cog(GraphCog(bot))
