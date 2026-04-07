"""Implementation for the #halogroups command."""

import csv
import io
from datetime import datetime
from typing import Dict

import discord
from discord.ext import commands


class HaloGroupsCommandMixin:
    @commands.command(name="halogroups", help="Show co-play communities and overlap matrix. Usage: #halogroups <gamertag>")
    async def show_halogroups(self, ctx: commands.Context, *inputs):
        """Show detected co-play communities around a player and export overlap CSVs."""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#halogroups GAMERTAG`")
            return

        gamertag = " ".join(inputs)
        min_matches = 2
        max_nodes = 60

        loading_embed = discord.Embed(
            title="Building Co-play Communities...",
            description=f"Analyzing co-play groups for **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now(),
        )
        loading_msg = await ctx.send(embed=loading_embed)

        try:
            api = self._get_api_client()
            xuid = await api.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.delete()
                await ctx.send(f"Could not find player **{gamertag}**")
                return

            player = self.db.get_player(xuid)
            if not player:
                await loading_msg.delete()
                await ctx.send(
                    f"**{gamertag}** is not in the graph database. Run `#crawlfriends` and `#crawlgames` first."
                )
                return

            active_min_matches = min_matches
            threshold_relaxed = False

            neighbors = self.db.get_coplay_neighbors(xuid, min_matches=active_min_matches, limit=max_nodes - 1)
            if not neighbors:
                relaxed_neighbors = self.db.get_coplay_neighbors(xuid, min_matches=1, limit=max_nodes - 1)
                if relaxed_neighbors:
                    neighbors = relaxed_neighbors
                    active_min_matches = 1
                    threshold_relaxed = True
                else:
                    await loading_msg.delete()
                    await ctx.send(
                        f"No co-play edges found for **{gamertag}**. Run `#crawlgames` and try again."
                    )
                    return

            node_map: Dict[str, Dict] = {
                xuid: {
                    "xuid": xuid,
                    "gamertag": gamertag,
                    "is_center": True,
                }
            }

            for row in neighbors:
                partner_xuid = row.get("partner_xuid")
                if not partner_xuid:
                    continue
                node_map[partner_xuid] = {
                    "xuid": partner_xuid,
                    "gamertag": row.get("gamertag") or partner_xuid,
                    "is_center": False,
                }

            all_xuids = list(node_map.keys())
            raw_edges = self.db.get_coplay_edges_within_set(all_xuids, min_matches=active_min_matches)

            # Collapse directional rows into one undirected edge with summed weights.
            aggregated_edges: Dict[tuple, Dict[str, object]] = {}
            for edge in raw_edges:
                src = edge.get("src_xuid")
                dst = edge.get("dst_xuid")
                if not src or not dst or src == dst:
                    continue
                key = tuple(sorted((src, dst)))
                bucket = aggregated_edges.setdefault(
                    key,
                    {
                        "src_xuid": key[0],
                        "dst_xuid": key[1],
                        "matches_together": 0,
                    },
                )
                bucket["matches_together"] += int(edge.get("matches_together") or 0)

            edges = [
                edge
                for edge in aggregated_edges.values()
                if int(edge.get("matches_together") or 0) >= active_min_matches
            ]

            if not edges:
                await loading_msg.delete()
                await ctx.send(
                    f"Co-play data exists for **{gamertag}**, but no edges met the current threshold ({active_min_matches})."
                )
                return

            import networkx as nx

            G = nx.Graph()
            for xuid_key, node in node_map.items():
                G.add_node(xuid_key, gamertag=node.get("gamertag") or xuid_key)

            for edge in edges:
                src = edge.get("src_xuid")
                dst = edge.get("dst_xuid")
                weight = int(edge.get("matches_together") or 0)
                if not src or not dst or src == dst or weight <= 0:
                    continue
                if G.has_edge(src, dst):
                    G[src][dst]["weight"] += weight
                else:
                    G.add_edge(src, dst, weight=weight)

            if G.number_of_nodes() < 2 or G.number_of_edges() < 1:
                await loading_msg.delete()
                await ctx.send(f"Not enough connected co-play data to compute groups for **{gamertag}**.")
                return

            communities = list(nx.algorithms.community.greedy_modularity_communities(G, weight="weight"))
            communities = sorted((set(c) for c in communities), key=lambda c: (-len(c), sorted(c)[0]))

            community_of: Dict[str, int] = {}
            for idx, members in enumerate(communities, start=1):
                for member in members:
                    community_of[member] = idx

            group_count = len(communities)
            matrix_weights = [[0 for _ in range(group_count)] for _ in range(group_count)]
            matrix_edges = [[0 for _ in range(group_count)] for _ in range(group_count)]

            for u, v, data in G.edges(data=True):
                gu = community_of.get(u)
                gv = community_of.get(v)
                if not gu or not gv:
                    continue
                i = gu - 1
                j = gv - 1
                w = int(data.get("weight") or 0)
                matrix_weights[i][j] += w
                matrix_weights[j][i] += w
                matrix_edges[i][j] += 1
                matrix_edges[j][i] += 1

            strongest_links = []
            for i in range(group_count):
                for j in range(i + 1, group_count):
                    if matrix_weights[i][j] <= 0:
                        continue
                    strongest_links.append((i + 1, j + 1, matrix_weights[i][j], matrix_edges[i][j]))
            strongest_links.sort(key=lambda item: item[2], reverse=True)

            embed = discord.Embed(
                title=f"Halo Groups: {gamertag}",
                description=(
                    "Detected co-play communities from weighted shared-match edges."
                    + (" Using fallback threshold of 1 shared match." if threshold_relaxed else "")
                ),
                colour=0x1ABC9C,
                timestamp=datetime.now(),
            )
            embed.add_field(
                name="Summary",
                value=(
                    f"Nodes: **{G.number_of_nodes()}**\n"
                    f"Edges: **{G.number_of_edges()}**\n"
                    f"Groups: **{group_count}**\n"
                    f"Min edge weight: **{active_min_matches}**"
                ),
                inline=True,
            )

            top_group_lines = []
            for idx, members in enumerate(communities[:5], start=1):
                names = sorted((node_map.get(m, {}).get("gamertag") or m) for m in members)
                preview = ", ".join(names[:6])
                if len(names) > 6:
                    preview += f", +{len(names) - 6} more"
                top_group_lines.append(f"G{idx} ({len(members)}): {preview}")
            embed.add_field(
                name="Top Groups",
                value="\n".join(top_group_lines) if top_group_lines else "No groups",
                inline=False,
            )

            if strongest_links:
                lines = [
                    f"G{a} <-> G{b}: {weight} shared matches across {edge_count} links"
                    for a, b, weight, edge_count in strongest_links[:6]
                ]
                embed.add_field(name="Strongest Inter-Group Links", value="\n".join(lines), inline=False)
            else:
                embed.add_field(
                    name="Strongest Inter-Group Links",
                    value="No cross-group links were detected (single cluster or disconnected communities).",
                    inline=False,
                )

            overlap_buf = io.StringIO()
            overlap_writer = csv.writer(overlap_buf)
            overlap_writer.writerow(["Group"] + [f"G{i}" for i in range(1, group_count + 1)])
            for i in range(group_count):
                overlap_writer.writerow([f"G{i + 1}"] + matrix_weights[i])

            members_buf = io.StringIO()
            members_writer = csv.writer(members_buf)
            members_writer.writerow(["group_id", "xuid", "gamertag", "is_center"])
            for idx, members in enumerate(communities, start=1):
                for member_xuid in sorted(members):
                    members_writer.writerow([
                        idx,
                        member_xuid,
                        node_map.get(member_xuid, {}).get("gamertag") or member_xuid,
                        int(member_xuid == xuid),
                    ])

            overlap_file = discord.File(
                io.BytesIO(overlap_buf.getvalue().encode("utf-8")),
                filename=f"halogroups_overlap_{xuid}.csv",
            )
            members_file = discord.File(
                io.BytesIO(members_buf.getvalue().encode("utf-8")),
                filename=f"halogroups_members_{xuid}.csv",
            )

            embed.set_footer(
                text="Attached CSVs: overlap matrix (shared matches) and full group memberships"
            )

            await loading_msg.delete()
            await ctx.send(embed=embed, files=[overlap_file, members_file])

        except Exception as e:
            try:
                await loading_msg.delete()
            except Exception:
                pass
            await ctx.send(f"Error showing halogroups: {str(e)}")
            raise
