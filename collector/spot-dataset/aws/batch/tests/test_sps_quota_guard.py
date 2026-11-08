import ast
from pathlib import Path
from types import SimpleNamespace
import pytest
from botocore.exceptions import ClientError


def test_configuration_quota_stops_without_rotating_accounts():
    path = Path(__file__).parents[1] / 'sps/collect_sps.py'
    tree = ast.parse(path.read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    branch = next(n for n in ast.walk(main) if isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == 'is_first_collection' and any(isinstance(x, ast.For) for x in n.body))
    calls = []
    class StopProbe(BaseException):
        pass
    def limited(args):
        calls.append(args[0])
        if len(calls) > 6:
            raise StopProbe()
        raise ClientError({'Error': {'Code': 'MaxConfigLimitExceeded'}}, 'GetSpotPlacementScores')
    class Rows:
        def __getitem__(self, key):
            return key
    ns = dict(is_first_collection=True, workload=[['scenario']], credentials=SimpleNamespace(iloc=Rows()),
              current_credential_index=0, init_credential_index=0, credential_range_end=3,
              target_capacity=10, query_sps=limited, df_list=[], tc_key='10', workload_credential_mapping={},
              botocore=SimpleNamespace(exceptions=SimpleNamespace(ClientError=ClientError)), print=lambda *a: None)
    with pytest.raises(ClientError):
        exec(compile(ast.Module(body=[branch], type_ignores=[]), str(path), 'exec'), ns)
    assert calls == [0]
