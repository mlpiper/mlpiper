import json
import pprint

from parallelm.mlops.common.string_ops import mask_passwords


def test_mask_passwords_simple():
    originalJson = """{"paSsword": "123\\"qwe"}"""
    expected = """{"password": "*****"}"""

    assert expected.lower() == mask_passwords(originalJson).lower()


def test_mask_passwords_complex_keyword():
    original = """{"key1": "key1-value", "sqlPassWoRd": "123\\"qwe"}"""

    masked_str = mask_passwords(json.loads(original))
    assert "*****" in masked_str
    assert '123"qwe' not in masked_str


def test_mask_passwords_full_json():
    originalJson = """
    {
        "name": "Someone",
        "pipe": [
            {
                "name": "comp6",
                "id": 123,
                "type": "data",
                "parents": [
                    { "parent": 1, "output": 0 },
                    { "parent": 2, "output": 0 }
                ],
                "arguments": [
                    {
                        "key1": "key1-value", "sqlPassword": "123\\"qwe",
                        "key2": "key2-value",
                        "password": "123qweassdzc*&^"
                    }
                ]
            }
        ]
    }"""

    expected = """
    {
        "name": "Someone",
        "pipe": [
            {
                "name": "comp6",
                "id": 123,
                "type": "data",
                "parents": [
                    { "parent": 1, "output": 0 },
                    { "parent": 2, "output": 0 }
                ],
                "arguments": [
                    {
                        "key1": "key1-value", "sqlPassword": "*****",
                        "key2": "key2-value",
                        "password": "*****"
                    }
                ]
            }
        ]
    }"""

    assert expected == mask_passwords(originalJson)


def test_mask_passwords_dict():
    d = {u'pipeline': u'{"name":"aaa","engineType":"PySpark","systemConfig":{"statsDBHost":"",'
                      u'"statsDBPort":0,"workflowInstanceId":"","statsMeasurementID":"","modelFileSinkPath":"",'
                      u'"healthStatFilePath":"","mlObjectSocketHost":"","mlObjectSocketSourcePort":0,'
                      u'"mlObjectSocketSinkPort":0,"socketSourcePort":0,"socketSinkPort":0,"enableHealth":true},'
                      u'"pipe":[{"name":"Number generator (DataFrame) (0e643516349b74a273894b3013698f3e2a20a2e4)",'
                      u'"id":0,"type":"num_gen_df_0e643516349b74a273894b3013698f3e2a20a2e4","parents":[],'
                      u'"arguments":{"num_samples":3333,"password":"123qwe"}},'
                      u'{"name":"Number generator (DataFrame) (0e643516349b74a273894b3013698f3e2a20a2e4)","id":1,'
                      u'"type":"num_gen_df_0e643516349b74a273894b3013698f3e2a20a2e4","parents":[],'
                      u'"arguments":{"num_samples":4444,"password":"456ert"}},'
                      u'{"name":"PI calculation (DataFrame) (0e643516349b74a273894b3013698f3e2a20a2e4)",'
                      u'"id":2,"type":"pi_calc_df_0e643516349b74a273894b3013698f3e2a20a2e4",'
                      u'"parents":[{"parent":0,"output":0,"input":0},{"parent":1,"output":0,"input":1}],'
                      u'"arguments":{}}]}'}

    masked = mask_passwords(d)

    assert '123qwe' not in masked
    assert '456ert' not in masked
