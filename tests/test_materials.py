import json,unittest,tempfile,shutil
from decimal import Decimal
from pathlib import Path
import pyarrow.parquet as pq
from materials.physical import load_inventory
ROOT=Path(__file__).resolve().parents[1]

class InputMaterials(unittest.TestCase):
    def test_real_files_and_independent_values(self):
        records=load_inventory(ROOT/"inputs")
        self.assertTrue(records)
        seed=json.loads((ROOT/"inputs/seed.json").read_text(encoding="utf-8"))
        for name,rows in seed.items():
            self.assertEqual(pq.read_table(ROOT/"inputs"/(name+".parquet")).to_pylist(),rows)

    def test_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/"inputs";shutil.copytree(ROOT/"inputs",target)
            inv=json.loads((target/"inventory.json").read_text(encoding="utf-8"))
            p=target/inv["files"][0]["path"];p.write_bytes(p.read_bytes()+b"x")
            with self.assertRaisesRegex(ValueError,"摘要"):load_inventory(target)

    def test_business_material_shapes(self):
        seed=json.loads((ROOT/"inputs/seed.json").read_text(encoding="utf-8"))
        actions=json.loads((ROOT/"inputs/actions.json").read_text(encoding="utf-8"))
        jobs=json.loads((ROOT/'inputs/jobs.json').read_text(encoding='utf-8'))
        self.assertEqual(jobs['balance']['depends'][1]['offsets'],[-1])
        self.assertEqual(jobs['turnover']['depends'][0]['offsets'],list(range(-6,1)))
        self.assertEqual(actions[1]['rows'],[])

    def test_provided_sql_is_executable(self):
        import duckdb
        jobs=json.loads((ROOT/"inputs/jobs.json").read_text(encoding="utf-8"))
        with duckdb.connect() as con:
            con.read_parquet(str(ROOT/"inputs/raw_day.parquet")).create_view("raw_day")
            con.execute("create temp table daily as "+jobs["daily"]["sql"],{"d":"2026-10-01"})
            con.execute("create temp table previous_balance as select day,site,cast(closing as decimal(18,3)) closing from read_parquet(?)",[str(ROOT/"inputs/opening.parquet")])
            balance=con.execute(jobs["balance"]["sql"],{"d":"2026-10-01"}).fetchall()
            self.assertEqual(len(balance),1)
            con.execute("create temp table balance as "+jobs["balance"]["sql"],{"d":"2026-10-01"})
            self.assertEqual(len(con.execute(jobs["turnover"]["sql"],{"d":"2026-10-01"}).fetchall()),1)
