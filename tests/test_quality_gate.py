#aqui probamos que los datos invalidos no se puedan cargar
import sys
from pathlib import Path
import unittest
import shutil
import os
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
import pipeline_india as p

class QualityGate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source_run = os.environ.get('INDIA_TEST_RUN_ID')
        if not cls.source_run:
            approvals = sorted((p.ROOT/'data/runs').glob('*/approval.json'), key=lambda f:f.stat().st_mtime)
            approvals = [f for f in approvals if not f.parent.name.startswith('test_')]
            if not approvals:
                raise unittest.SkipTest('Ejecuta primero el notebook para generar un lote aprobado')
            cls.source_run = json.loads(approvals[-1].read_text())['run_id']
    def test_negative_precipitation_and_changed_file_are_blocked(self):
        original = p.approved_dataframe(self.source_run)
        bad = original.copy()
        bad.loc[0,'precipitation_sum'] = -1
        path = p.workspace('test_quality_failure')
        self.assertFalse(p.validate_dataframe(bad,path/'validation.json'))
        bad.to_csv(path/'enriched.csv',index=False)
        with self.assertRaises(FileNotFoundError):
            p.approved_dataframe('test_quality_failure')
        approved = p.workspace('test_changed_file')
        source = p.workspace(self.source_run)
        for name in ['enriched.csv','approval.json','validation.json']:
            shutil.copy2(source/name,approved/name)
        approval = __import__('json').loads((approved/'approval.json').read_text())
        approval['run_id'] = 'test_changed_file'
        p.save_json(approved/'approval.json',approval)
        self.assertEqual(len(p.approved_dataframe('test_changed_file')),p.NROWS)
        with (approved/'enriched.csv').open('a') as stream:
            stream.write('\n')
        with self.assertRaises(ValueError):
            p.approved_dataframe('test_changed_file')

if __name__=='__main__':
    unittest.main()
