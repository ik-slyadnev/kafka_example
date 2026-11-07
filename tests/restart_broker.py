"""Run on the host with the trainer started; stops and restarts its Kafka broker."""
import json
from pathlib import Path
import subprocess
from uuid import uuid4

repo=Path(__file__).resolve().parents[1]
topic='restart-assignment-'+uuid4().hex

def run(*args):
    result=subprocess.run(args,cwd=repo,text=True,capture_output=True,timeout=120)
    assert result.returncode == 0, (args,result.stdout,result.stderr)
    return result.stdout

project=run('docker','inspect','--format','{{ index .Config.Labels "com.docker.compose.project" }}','kafka').strip()
assert project, 'Kafka container must belong to a Compose project'

try:
    run('docker','exec','python-tools','python','topic_manager.py','--create',topic,'--partitions','3')
    run('docker','exec','python-tools','python','producer.py','--topic',topic,'--count','10','--interval','0')
    run('docker','compose','-p',project,'stop','kafka')
    state=json.loads(run('docker','inspect','kafka'))[0]['State']['Running']
    assert state is False
    run('docker','compose','-p',project,'up','-d','--wait','--wait-timeout','90','kafka')
    output=run('docker','exec','python-tools','python','consumer.py','--topic',topic,'--from-beginning','--limit','10')
    assert output.count('Значение:')==10,output
    values=[json.loads(line.split('Значение: ',1)[1]) for line in output.splitlines() if line.startswith('Значение: ')]
    assert sorted(v['id'] for v in values)==list(range(10))
    print('PASS practice 4: sent 10 messages, stopped and restarted Kafka, read all 10 IDs without loss')
finally:
    run('docker','exec','python-tools','python','topic_manager.py','--delete',topic)
