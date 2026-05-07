import duckdb
import os

DB_PATH    = os.path.join(os.path.dirname(__file__), '..', 'ipl_warehouse.duckdb')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), '..', 'powerbi')

con = duckdb.connect(DB_PATH)

tables = ['top_batsmen', 'bowler_phase', 'toss_impact', 'team_venue']

for t in tables:
    df = con.execute(f'SELECT * FROM {t}').df()
    path = os.path.join(EXPORT_DIR, f'{t}.csv')
    df.to_csv(path, index=False)
    print(f'Exported {t}: {len(df)} rows → {path}')

con.close()
