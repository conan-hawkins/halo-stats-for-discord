import sqlite3

db = sqlite3.connect('data/halo_social_graph.db')
db.row_factory = sqlite3.Row
cursor = db.cursor()

# Get top 3 by total friends (degree)
cursor.execute('''
    SELECT gp.xuid, gp.gamertag, COUNT(*) as total_friends
    FROM graph_friends gf
    JOIN graph_players gp ON gf.src_xuid = gp.xuid
    WHERE gp.halo_active = 1
    GROUP BY gf.src_xuid
    ORDER BY total_friends DESC
    LIMIT 3
''')
top_players = cursor.fetchall()

print('Top 3 players:')
for i, row in enumerate(top_players, 1):
    xuid = row['xuid']
    gamertag = row['gamertag']
    total_friends = row['total_friends']
    # Count how many of their friends are Halo-active
    cursor.execute('''
        SELECT COUNT(*) FROM graph_friends gf
        JOIN graph_players gp2 ON gf.dst_xuid = gp2.xuid
        WHERE gf.src_xuid = ? AND gp2.halo_active = 1
    ''', (xuid,))
    halo_friends = cursor.fetchone()[0]
    print(f"{i}. {gamertag} - {total_friends} total friends, {halo_friends} Halo-active friends")

db.close()
