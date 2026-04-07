"""Runtime helpers for the standalone #network command flow."""

import asyncio
import io
from datetime import datetime
from typing import Dict, Optional

import discord
from discord.ext import commands

from src.api import api_client
from src.bot.cogs.graph_commands.display.network.ui import NetworkFilterView, NetworkNodeInfoView


async def execute_show_network(cog, ctx: commands.Context, db, inputs: tuple) -> None:
    """Execute the #network command using the provided cog and database."""
    if not inputs:
        await ctx.send("Please provide a gamertag. Example: `#network GAMERTAG`")
        return

    gamertag = " ".join(inputs)

    loading_embed = discord.Embed(
        title="Building Network Graph...",
        description=f"Fetching data for **{gamertag}**",
        colour=0xFFA500,
        timestamp=datetime.now(),
    )
    loading_msg = await ctx.send(embed=loading_embed)

    try:
        # Resolve gamertag
        xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
        if not xuid:
            await loading_msg.delete()
            await ctx.send(f"Could not find player **{gamertag}**")
            return

        # Get player info
        player = db.get_player(xuid)
        if not player:
            await loading_msg.delete()
            await ctx.send(f"**{gamertag}** is not in the graph database. Run a crawl first.")
            return

        # Get friends
        halo_friends = db.get_halo_friends(xuid)
        halo_flagged_count = len(halo_friends)
        all_friends = db.get_friends(xuid)
        features = db.get_halo_features(xuid)

        last_crawled_raw = player.get("last_crawled")
        if last_crawled_raw:
            try:
                last_crawled_dt = datetime.fromisoformat(last_crawled_raw)
                crawl_status = f"Last crawl: {last_crawled_dt.strftime('%Y-%m-%d %H:%M')}"
            except (ValueError, TypeError):
                crawl_status = f"Last crawl: {last_crawled_raw}"
        else:
            crawl_status = "Player not crawled"

        # Keep graph nodes to verified Halo-active friends (have recorded Halo matches).
        halo_friends = [f for f in halo_friends if (f.get("matches_played") or 0) > 0]
        halo_verified_count = len(halo_friends)
        friends_with_stats = [f for f in halo_friends if f.get("matches_played") is not None]

        # Social group size comes from persisted snapshot on each friend node.
        # Lazily backfill if no snapshot exists yet for a friend.
        for friend in halo_friends:
            friend_xuid = friend.get("dst_xuid")
            if not friend_xuid:
                continue

            if friend.get("inference_updated_at") is None:
                snapshot = db.refresh_inferred_group_snapshot(friend_xuid)
                friend["social_group_size"] = int(snapshot.get("social_group_size") or 0)
                friend["group_size_inferred"] = bool(snapshot.get("social_group_size_inferred"))
                friend["group_size_source"] = snapshot.get("social_group_source") or "unknown"
            else:
                friend["social_group_size"] = int(friend.get("social_group_size") or 0)
                friend["group_size_inferred"] = bool(friend.get("social_group_size_inferred"))
                friend["group_size_source"] = friend.get("social_group_source") or "unknown"

        # Build summary embed
        embed = discord.Embed(
            title=f"Network: {gamertag} ({crawl_status})",
            colour=0x3498DB,
            timestamp=datetime.now(),
        )

        embed.add_field(
            name="Summary",
            value=(
                f"Total friends: **{len(all_friends)}**\n"
                f"Halo flagged: **{halo_flagged_count}**\n"
                f"Halo-active friends (verified): **{halo_verified_count}**\n"
                f"Node depth (from seed): **{player.get('crawl_depth', '?')}**\n"
                f"Friends with stats: **{len(friends_with_stats)}/{halo_verified_count}**"
            ),
            inline=True,
        )

        if features and (features.get("matches_played") or 0) > 0:
            embed.add_field(
                name="Player Stats",
                value=(
                    f"K/D: {features.get('kd_ratio') or 0:.2f}\n"
                    f"Win Rate: {features.get('win_rate') or 0:.1f}%\n"
                    f"Matches: {features.get('matches_played') or 0}"
                ),
                inline=True,
            )

        if halo_friends:
            sorted_friends = sorted(
                halo_friends,
                key=lambda x: ((x.get("social_group_size") or 0), (x.get("matches_played") or 0)),
                reverse=True,
            )
            section_title = "Top Halo Friends (by Social Group Size)"

            top_str = ""
            for f in sorted_friends[:5]:
                gt = f.get("gamertag") or f.get("dst_xuid", "Unknown")
                kd = f.get("kd_ratio")
                kd_str = f"{kd:.2f}" if kd is not None else "N/A"
                group_size = f.get("social_group_size") or 0
                top_str += f"**{gt}**: Group Size {group_size}, K/D {kd_str}\n"
            embed.add_field(
                name=section_title,
                value=top_str or "None",
                inline=False,
            )

        embed.set_footer(text="XUID: {xuid} | Gold=center | YlOrRd=group size | Greens=link strength | Green outline=direct | Orange outline=inferred | Red outline=private".format(xuid=xuid))

        await loading_msg.delete()

        if not halo_friends:
            await ctx.send(embed=embed)
            return

        max_friends = 60
        friends_to_show = halo_friends[:max_friends]

        node_map: Dict[str, Dict] = {
            xuid: {
                "xuid": xuid,
                "gamertag": gamertag,
                "group_size": len(halo_friends),
                "kd_ratio": features.get("kd_ratio") if features else None,
                "win_rate": features.get("win_rate") if features else None,
                "matches_played": features.get("matches_played") if features else None,
                "is_center": True,
            }
        }
        for f in friends_to_show:
            fxuid = f.get("dst_xuid")
            if not fxuid:
                continue
            node_map[fxuid] = {
                "xuid": fxuid,
                "gamertag": f.get("gamertag") or fxuid,
                "group_size": f.get("social_group_size") or 0,
                "group_size_inferred": bool(f.get("group_size_inferred")),
                "group_size_source": f.get("group_size_source") or "direct",
                "kd_ratio": f.get("kd_ratio"),
                "win_rate": f.get("win_rate"),
                "matches_played": f.get("matches_played"),
                "is_mutual": bool(f.get("is_mutual")),
                "is_center": False,
            }

        # Render graph image in a thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        buf = await loop.run_in_executor(
            None,
            lambda: cog._render_network_graph(xuid, gamertag, friends_to_show, features),
        )
        file = discord.File(fp=buf, filename="network.png")
        requester_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
        filter_view = NetworkFilterView(
            cog=cog,
            requester_id=requester_id,
            center_xuid=xuid,
            center_gamertag=gamertag,
            halo_friends=friends_to_show,
            center_features=features,
            base_embed=embed,
        )
        embed.description = filter_view._build_description(controls_active=True)
        embed.set_image(url="attachment://network.png")
        graph_message = await ctx.send(
            embed=embed,
            file=file,
            view=filter_view,
        )
        filter_view.message = graph_message

        if len(node_map) > 1:
            await ctx.send(
                "Use the selector below for node details:",
                view=NetworkNodeInfoView(node_map=node_map, requester_id=requester_id, db=db),
            )
            await ctx.send("Use controls on the graph message to switch layout and apply filters. If controls time out, use **Refresh Controls** on that same message.")

    except Exception as e:
        try:
            await loading_msg.delete()
        except Exception:
            pass
        await ctx.send(f"Error showing network: {str(e)}")
        raise


def render_network_graph(
    db,
    center_xuid: str,
    center_gamertag: str,
    halo_friends: list,
    center_features: Optional[dict],
    clustered: bool = False,
    min_group_size: int = 0,
    min_link_strength: float = 1.0,
) -> io.BytesIO:
    """Render the friend network as a PNG and return a BytesIO buffer (sync)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as path_effects
    import networkx as nx
    from matplotlib.lines import Line2D

    max_friends = 60
    friends_to_show = [
        f for f in halo_friends[:max_friends]
        if (f.get("social_group_size") or 0) >= min_group_size
    ]

    friend_xuids = [f["dst_xuid"] for f in friends_to_show]
    all_xuids = [center_xuid] + friend_xuids

    # Cross-edges between friends (not touching center node)
    cross_edges_raw = db.get_edges_within_set(all_xuids)
    cross_edge_set = {
        (e["src_xuid"], e["dst_xuid"])
        for e in cross_edges_raw
        if e["src_xuid"] != center_xuid and e["dst_xuid"] != center_xuid
    }

    G = nx.Graph()

    # Center node
    center_group_size = len(halo_friends)
    G.add_node(center_xuid, label=center_gamertag, is_center=True, group_size=center_group_size)

    # Friend nodes + spoke edges
    for f in friends_to_show:
        fxuid = f["dst_xuid"]
        fgt = f.get("gamertag") or fxuid[:10]
        G.add_node(
            fxuid,
            label=fgt,
            is_center=False,
            group_size=f.get("social_group_size") or 0,
            group_size_inferred=bool(f.get("group_size_inferred")),
        )
        G.add_edge(center_xuid, fxuid, weight=1.0)

    # Cross-edges
    for src, dst in cross_edge_set:
        if G.has_node(src) and G.has_node(dst) and not G.has_edge(src, dst):
            G.add_edge(src, dst, weight=1.4)

    if min_link_strength > 1:
        degree_map = dict(G.degree())
        edges_to_remove = []
        for u, v in G.edges():
            strength = min(degree_map.get(u, 0), degree_map.get(v, 0))
            if strength < min_link_strength:
                edges_to_remove.append((u, v))
        if edges_to_remove:
            G.remove_edges_from(edges_to_remove)

    # Keep only nodes reachable from the center after filtering.
    if G.has_node(center_xuid):
        reachable = nx.node_connected_component(G, center_xuid)
        nodes_to_remove = [n for n in G.nodes if n not in reachable]
        if nodes_to_remove:
            G.remove_nodes_from(nodes_to_remove)

    # Layout: spread nodes further for readability in dense networks.
    k = 5.8 / max(1, len(G.nodes) ** 0.5)
    pos = nx.spring_layout(G, seed=42, k=k, iterations=120, weight="weight")

    if clustered and len(G.nodes) >= 5 and len(G.edges) >= 4:
        communities = list(nx.algorithms.community.greedy_modularity_communities(G, weight="weight"))
        if len(communities) > 1:
            cluster_of = {}
            for cid, members in enumerate(communities):
                for n in members:
                    cluster_of[n] = cid

            cluster_graph = nx.Graph()
            for cid in range(len(communities)):
                cluster_graph.add_node(cid)
            for u, v, data in G.edges(data=True):
                cu = cluster_of.get(u)
                cv = cluster_of.get(v)
                if cu is None or cv is None or cu == cv:
                    continue
                w = data.get("weight", 1.0)
                if cluster_graph.has_edge(cu, cv):
                    cluster_graph[cu][cv]["weight"] += w
                else:
                    cluster_graph.add_edge(cu, cv, weight=w)

            cluster_k = 1.9 / max(1, len(cluster_graph.nodes()) ** 0.5)
            cluster_pos = nx.spring_layout(cluster_graph, seed=42, k=cluster_k, iterations=100, weight="weight")

            clustered_pos = {}
            for cid, members in enumerate(communities):
                sub = G.subgraph(members)
                local_k = 1.3 / max(1, len(sub.nodes()) ** 0.5)
                local_pos = nx.spring_layout(sub, seed=42, k=local_k, iterations=70, weight="weight")
                center = cluster_pos.get(cid, (0.0, 0.0))
                radius = 0.18 + 0.025 * min(10, len(sub.nodes()))
                for n, coords in local_pos.items():
                    clustered_pos[n] = (
                        center[0] + coords[0] * radius,
                        center[1] + coords[1] * radius,
                    )

            if len(clustered_pos) == len(G.nodes):
                pos = clustered_pos

    # Stretch normalized layout to fill most of the canvas width/height.
    x_values = [p[0] for p in pos.values()]
    y_values = [p[1] for p in pos.values()]
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(y_values), max(y_values)
    x_span = (x_max - x_min) or 1.0
    y_span = (y_max - y_min) or 1.0

    x_left, x_right = 0.02, 0.84
    # Reserve extra top room so sparse-node layouts do not sit under legend/colorbars.
    y_bottom, y_top = 0.03, 0.87
    pos = {
        node: (
            x_left + ((coords[0] - x_min) / x_span) * (x_right - x_left),
            y_bottom + ((coords[1] - y_min) / y_span) * (y_top - y_bottom),
        )
        for node, coords in pos.items()
    }

    # Single colour scale for group size; confidence comes from outlines.
    friend_groups = [
        G.nodes[n]["group_size"]
        for n in friend_xuids
        if G.has_node(n)
    ]

    def _safe_norm(values: list):
        if not values:
            return mcolors.Normalize(vmin=0, vmax=1)
        vmin = min(values)
        vmax = max(values)
        if vmin == vmax:
            vmax = vmin + 1
        return mcolors.Normalize(vmin=vmin, vmax=vmax)

    group_colormap = cm.YlOrRd
    link_colormap = cm.Greens
    group_norm = _safe_norm(friend_groups)

    node_colors = []
    node_sizes = []
    node_edge_colors = []
    node_linewidths = []
    labels = {}
    for n in G.nodes:
        data = G.nodes[n]
        labels[n] = data["label"]
        if data.get("is_center"):
            node_colors.append("#FFD700")
            node_sizes.append(620)
            node_edge_colors.append("white")
            node_linewidths.append(1.0)
        else:
            node_colors.append(group_colormap(group_norm(data["group_size"])))
            node_sizes.append(120 + G.degree(n) * 28)
            if data.get("group_size_inferred"):
                node_edge_colors.append("#FFA500")
                node_linewidths.append(2.4)
            elif (data.get("group_size") or 0) == 0:
                node_edge_colors.append("#FF3B30")
                node_linewidths.append(2.4)
            else:
                node_edge_colors.append("#22dd22")
                node_linewidths.append(1.2)

    visible_labels = labels

    # Plot
    bg = "#1a1a2e"
    fig, ax = plt.subplots(figsize=(14.5, 11), facecolor=bg)
    fig.subplots_adjust(left=0.02, right=0.98, top=0.90, bottom=0.06)
    ax.set_facecolor(bg)

    # Spoke edges (center -> friends)
    spoke_edges = [(u, v) for u, v in G.edges() if center_xuid in (u, v)]
    cross_list = [(u, v) for u, v in G.edges() if center_xuid not in (u, v)]

    # Node link strength in rendered graph: use endpoint degree on each edge.
    edge_strengths = [min(G.degree(u), G.degree(v)) for u, v in G.edges()]
    link_norm = _safe_norm(edge_strengths)

    def _edge_style(edges: list, alpha_base: float):
        colors = []
        widths = []
        for u, v in edges:
            strength = min(G.degree(u), G.degree(v))
            colors.append(link_colormap(link_norm(strength)))
            widths.append(0.7 + 1.8 * link_norm(strength))
        return colors, widths, alpha_base

    spoke_colors, spoke_widths, spoke_alpha = _edge_style(spoke_edges, 0.55)
    cross_colors, cross_widths, cross_alpha = _edge_style(cross_list, 0.35)

    nx.draw_networkx_edges(G, pos, edgelist=spoke_edges, edge_color=spoke_colors, width=spoke_widths, alpha=spoke_alpha, ax=ax)
    if cross_list:
        nx.draw_networkx_edges(G, pos, edgelist=cross_list, edge_color=cross_colors, width=cross_widths, alpha=cross_alpha, ax=ax)

    nx.draw_networkx_nodes(
        G,
        pos,
        node_color=node_colors,
        node_size=node_sizes,
        linewidths=node_linewidths,
        edgecolors=node_edge_colors,
        ax=ax,
    )
    label_artists = nx.draw_networkx_labels(
        G,
        pos,
        labels=visible_labels,
        font_size=6,
        font_color="white",
        ax=ax,
    )
    for text_artist in label_artists.values():
        text_artist.set_path_effects([
            path_effects.Stroke(linewidth=2.2, foreground="black"),
            path_effects.Normal(),
        ])

    # Colourbars (group size + link strength), horizontal along the top beside legend.
    group_cax = fig.add_axes([0.36, 0.915, 0.25, 0.018])
    link_cax = fig.add_axes([0.66, 0.915, 0.25, 0.018])
    group_cax.set_facecolor(bg)
    link_cax.set_facecolor(bg)

    group_sm = cm.ScalarMappable(cmap=group_colormap, norm=group_norm)
    group_sm.set_array([])
    group_cbar = fig.colorbar(group_sm, cax=group_cax, orientation="horizontal")
    group_cbar.set_label("Group Size (YlOrRd: low -> high)", color="white", fontsize=8)
    group_cbar.ax.xaxis.set_tick_params(color="white", labelsize=7)
    plt.setp(group_cbar.ax.xaxis.get_ticklabels(), color="white")
    group_cbar.outline.set_edgecolor("white")

    link_sm = cm.ScalarMappable(cmap=link_colormap, norm=link_norm)
    link_sm.set_array([])
    link_cbar = fig.colorbar(link_sm, cax=link_cax, orientation="horizontal")
    link_cbar.set_label("Node Link Strength (Greens: weak -> strong)", color="white", fontsize=9)
    link_cbar.ax.xaxis.set_tick_params(color="white", labelsize=7)
    plt.setp(link_cbar.ax.xaxis.get_ticklabels(), color="white")
    link_cbar.outline.set_edgecolor("white")

    # Graph key for node semantics; outline-only meanings intentionally have no fill.
    legend_handles = [
        Line2D([0], [0], marker="o", color="none", label="Center Player", markerfacecolor="#FFD700", markeredgecolor="white", markeredgewidth=1.0, markersize=9),
        Line2D([0], [0], marker="o", color="none", label="Green Outline Only: direct friend data visible", markerfacecolor="none", markeredgecolor="#22dd22", markeredgewidth=2.2, markersize=8),
        Line2D([0], [0], marker="o", color="none", label="Orange Outline Only: inferred via reciprocal data", markerfacecolor="none", markeredgecolor="#FFA500", markeredgewidth=2.2, markersize=8),
        Line2D([0], [0], marker="o", color="none", label="Red Outline Only: private/empty friend list", markerfacecolor="none", markeredgecolor="#FF3B30", markeredgewidth=2.2, markersize=8),
        Line2D([0], [0], color=link_colormap(0.25), linewidth=1.2, label="Weaker Link Strength"),
        Line2D([0], [0], color=link_colormap(0.85), linewidth=2.6, label="Stronger Link Strength"),
        Line2D([0], [0], marker="o", color="none", label="Smaller Node (Lower Degree)", markerfacecolor="#cccccc", markeredgecolor="white", markeredgewidth=0.6, markersize=5),
        Line2D([0], [0], marker="o", color="none", label="Larger Node (Higher Degree)", markerfacecolor="#cccccc", markeredgecolor="white", markeredgewidth=0.6, markersize=10),
    ]
    legend = ax.legend(handles=legend_handles, loc="upper left", frameon=True, facecolor=bg, edgecolor="white", fontsize=8)
    for text in legend.get_texts():
        text.set_color("white")

    shown = len(friends_to_show)
    total = len(halo_friends)
    # Count private-list nodes (group_size=0) including those that were inferred as such.
    private_list_count = sum(1 for f in friends_to_show if (f.get("social_group_size") or 0) == 0)
    inferred_count = sum(1 for f in friends_to_show if f.get("group_size_inferred"))
    title = f"Halo Network: {center_gamertag}"
    if total > shown:
        title += f"  (showing {shown} of {total} Halo friends)"
    title += f"  |  Private-list nodes: {private_list_count}"
    title += f"  |  Inferred nodes: {inferred_count}"
    if min_group_size > 0 or min_link_strength > 1:
        title += f"  |  Filter N>={min_group_size}, E>={int(min_link_strength)}"
    if clustered:
        title += "  |  Layout: Clustered"
    # Place title at the bottom per user preference.
    fig.text(0.50, 0.015, title, color="white", fontsize=12, ha="center", va="bottom")
    ax.axis("off")
    # Keep margins minimal after explicit subplot placement.
    ax.margins(x=0.0, y=0.0)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor=bg)
    buf.seek(0)
    plt.close(fig)
    return buf
