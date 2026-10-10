"""Small supplied helpers. They do not compute or certify data quality."""

import hashlib, json, uuid

from datetime import datetime, timezone

from pathlib import Path

 

def sha256(path):

    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

 

def ratio(numerator, denominator):

    if denominator == 0:

        return dict(n=numerator, d=0, value=None, status='NOT_EVALUATED')

    if not 0 <= numerator <= denominator:

        raise ValueError('Invalid numerator or denominator')

    return dict(n=numerator, d=denominator,

                value=numerator/denominator, status='EVALUATED')

 

def new_run():

    return str(uuid.uuid4())

 

def lineage_event(kind, run_id, job, inputs, outputs, store_id):

    if kind not in {'START','COMPLETE','ABORT','FAIL'}:

        raise ValueError('Unsupported run event')

    def dataset(name):

        return {'namespace':'urn:bigdata:'+store_id, 'name':name}

    return dict(eventType=kind,

        eventTime=datetime.now(timezone.utc).isoformat(),

        producer='urn:vnu-uet:bigdata:lab2:student-pipeline',

        schemaURL='https://openlineage.io/spec/2-0-2/'
                  'OpenLineage.json#/$defs/RunEvent',

        run={'runId':run_id},

        job={'namespace':'vnu-uet.bigdata.lab2', 'name':job},

        inputs=[dataset(x) for x in inputs],

        outputs=[dataset(x) for x in outputs])

 

def append_event(path, event):

    with Path(path).open('a', encoding='utf-8') as f:

        f.write(json.dumps(event)+'\n')

 

def validate_event(event, schema_path):

    from jsonschema import Draft202012Validator, FormatChecker

    schema = json.loads(Path(schema_path).read_text())

    # The official root has RunEvent, DatasetEvent and JobEvent alternatives.

    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)