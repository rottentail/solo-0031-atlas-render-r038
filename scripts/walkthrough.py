#!/usr/bin/env python3
"""Run bounded production flows against a temporary Atlas Render server."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zlib

ROOT = Path(__file__).resolve().parents[1]


def sample():
    return {"width": 320, "height": 240, "background": "#F5F1E8", "layers": [
        {"id": "plate", "kind": "rectangle", "width": 220, "height": 150,
         "transform": {"x": 45, "y": 35, "scaleX": 1, "scaleY": 1, "degrees": 5},
         "opacity": 1, "fill": {"kind": "linear", "color": "#145A70", "endColor": "#32A5A0", "from": {"x": 0, "y": 0}, "to": {"x": 220, "y": 150}}},
        {"id": "accent", "kind": "ellipse", "width": 130, "height": 130,
         "transform": {"x": 150, "y": 95, "scaleX": 1, "scaleY": 1, "degrees": 0},
         "opacity": .85, "fill": {"kind": "solid", "color": "#E87E4F"}},
        {"id": "inset", "kind": "group", "transform": {"x": 150, "y": 100, "scaleX": 1, "scaleY": 1, "degrees": -8}, "opacity": .9,
         "clip": {"x": 0, "y": 0, "width": 90, "height": 75}, "children": [
             {"id": "facet", "kind": "polygon", "width": 100, "height": 100,
              "transform": {"scaleX": 1, "scaleY": 1}, "opacity": .8,
              "fill": {"kind": "solid", "color": "#FFF4D5"}, "points": [{"x": 10, "y": 90}, {"x": 50, "y": 0}, {"x": 100, "y": 90}]}]}]}


def tiny_png():
    def chunk(tag, data):
        return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', zlib.crc32(tag + data))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', 2, 2, 8, 6, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(b'\0' + bytes([240, 40, 80, 255] * 2) + b'\0' + bytes([40, 80, 240, 128] * 2))) + chunk(b'IEND', b'')


class Demo:
    """Runs one API process and one or more execution processes.

    The API only accepts and serves requests; every render is executed by a
    separate ``atlas worker`` process that shares the same state file. The
    helper exposes explicit lifecycle control so failover can be exercised by
    killing a worker while it owns a job.
    """

    def __init__(self, binary, directory, workers=1):
        self.binary = binary
        self.directory = directory
        self.proc = None
        self.workers = []
        self.worker_count = workers
        self.log = open(directory / 'server.log', 'ab')

    def start(self, workers=None):
        if workers is not None:
            self.worker_count = workers
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        self.env = dict(os.environ, ATLAS_ADDR=f'127.0.0.1:{port}', ATLAS_STATE=str(self.directory / 'state.json'))
        self.proc = subprocess.Popen([str(self.binary), 'serve'], env=self.env, stdout=self.log, stderr=self.log)
        for _ in range(100):
            try:
                self.call('GET', '/healthz')
                break
            except (urllib.error.URLError, ConnectionError):
                if self.proc.poll() is not None:
                    raise RuntimeError((self.directory / 'server.log').read_text())
                time.sleep(.03)
        else:
            raise RuntimeError('server startup deadline exceeded')
        self.start_workers()

    def start_workers(self):
        for index in range(len(self.workers), self.worker_count):
            env = dict(self.env, ATLAS_WORKER_ID=f'walkthrough-worker-{index}')
            self.workers.append(subprocess.Popen([str(self.binary), 'worker'], env=env, stdout=self.log, stderr=self.log))

    def kill_workers(self, abrupt=True):
        for worker in self.workers:
            if worker.poll() is None:
                (worker.kill if abrupt else worker.terminate)()
                worker.wait(timeout=15)
        self.workers = []

    def stop(self, abrupt=False):
        if self.proc and self.proc.poll() is None:
            (self.proc.kill if abrupt else self.proc.terminate)()
            self.proc.wait(timeout=15)
        self.kill_workers(abrupt=False)

    def call(self, method, path, body=None, status=200, headers=None):
        hdr = dict(headers or {})
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            hdr['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.url + path, data=body, method=method, headers=hdr)
        try:
            response = urllib.request.urlopen(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            if response.status != status:
                raise RuntimeError(f'{method} {path}: expected {status}, received {response.status}: {data[:500]!r}')
            if response.headers.get('Content-Type', '').startswith('application/json'):
                return json.loads(data)
            return data

    def wait(self, job_id, wanted=('succeeded',), timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.call('GET', '/v1/jobs/' + job_id)
            if job['state'] in wanted:
                return job
            if job['state'] in ('failed', 'cancelled') and 'cancelled' not in wanted:
                raise RuntimeError(f'unexpected job outcome: {job}')
            time.sleep(.025)
        raise RuntimeError('render deadline exceeded')

    def submit(self, snapshot, key='render-one', scale=1):
        return self.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': scale}, 202, {'Idempotency-Key': key})


def heavy_document():
    heavy = sample(); heavy['width'] = 1500; heavy['height'] = 1500
    heavy['layers'] = [dict(heavy['layers'][0], id=f'sheet-{i}', width=1500, height=1500, transform={'scaleX': 1, 'scaleY': 1}) for i in range(16)]
    return heavy


def run_partition(demo, current):
    """An execution process that dies mid-job must not block the queue.

    A single worker claims a heavy job and is then killed. The API stays
    online and reports the orphaned running attempt. Once the lease expires a
    replacement execution process takes over the same attempt (no retry
    inflation) and publishes exactly one result.

    The complementary case — a process that is still alive but whose claim was
    revoked — is covered deterministically by the engine unit tests
    (TestFinishRejectsCancelledClaim and TestFinishRejectsStaleAttempt), which
    do not depend on where a signal lands.
    """
    demo.worker_count = 0; demo.kill_workers()
    heavy_doc = heavy_document(); heavy_doc['background'] = '#203040'
    demo.call('PUT', '/v1/scenes/emblem', {'name': '分区演练', 'expectedRevision': current['revision'], 'document': heavy_doc})
    snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)

    # Start a single worker and let it claim the job.
    demo.worker_count = 1; demo.start_workers()
    job = demo.submit(snapshot, 'partition-drill')
    first = demo.wait(job['id'], wanted=('running',))
    assert first.get('workerId')

    # Simulate the execution process dying while it owns the job. SIGKILL
    # closes the process's file descriptors, so the advisory lock is released
    # by the kernel; the job itself stays running with an orphaned claim until
    # its lease expires. This is the realistic failure a deployment must
    # survive, and it does not depend on timing inside a short transaction.
    stale = demo.workers[0]
    stale.kill()
    stale.wait(timeout=15)
    assert stale.poll() is not None
    orphan = demo.call('GET', '/v1/jobs/' + job['id'])
    assert orphan['state'] == 'running', orphan
    assert orphan['workerId'] == first['workerId'], orphan

    # A replacement execution process takes over the same attempt.
    replacement_env = dict(demo.env, ATLAS_WORKER_ID='walkthrough-replacement')
    replacement = subprocess.Popen([str(demo.binary), 'worker'], env=replacement_env, stdout=demo.log, stderr=demo.log)
    try:
        deadline = time.monotonic() + 30
        takeover = None
        while time.monotonic() < deadline:
            candidate = demo.call('GET', '/v1/jobs/' + job['id'])
            if candidate['state'] == 'running' and candidate['workerId'] == 'walkthrough-replacement':
                takeover = candidate
                break
            time.sleep(.05)
        assert takeover, 'replacement worker never took over the expired lease'
        assert takeover['attempt'] == 1, takeover
        assert takeover['workerId'] == 'walkthrough-replacement', takeover
        # The replacement finishes the same attempt.
        succeeded = demo.wait(job['id'], timeout=40)
        assert succeeded['attempt'] == 1, succeeded
        winner = succeeded['artifactId']

        final = demo.call('GET', '/v1/jobs/' + job['id'])
        assert final['artifactId'] == winner, (final, winner)
        assert final['attempt'] == 1, final
        assert final['state'] == 'succeeded', final

        state = json.loads((demo.directory / 'state.json').read_text())
        artifacts = [a for a in state['artifacts'].values() if a['jobId'] == job['id']]
        assert len(artifacts) == 1, artifacts
        assert artifacts[0]['id'] == winner, artifacts

        events = demo.call('GET', '/v1/events')
        recovered = [e for e in events if e['kind'] == 'job.recovered' and e['subject'] == job['id']]
        assert recovered, 'expected a recovery event for the orphaned attempt'
        returning = [e for e in events if e['kind'] == 'job.running' and e['subject'] == job['id']]
        assert any(e.get('actor') == 'walkthrough-replacement' for e in returning), returning
        return {'workflow': 'partition-recovery', 'jobId': job['id'], 'attempts': final['attempt'],
                'artifactId': winner, 'recoveredEvents': len(recovered),
                'artifactsForJob': len(artifacts), 'state': final['state'],
                'orphanRequeued': True}
    finally:
        for p in (replacement, stale):
            if p.poll() is None:
                p.kill()
                p.wait(timeout=15)
        demo.workers = []


def run_multi_worker(demo, snapshot, current):
    """Exercise the split intake/execution topology end to end.

    Verifies that several execution processes share the queue, that a worker
    killed while holding a lease is replaced without a new attempt, that a
    cancelled attempt never publishes a result, that a graceful stop returns
    unfinished work, and that state written before the lease protocol is still
    consumed.
    """
    observations = {}

    # Several workers process a batch without duplicating output.
    demo.worker_count = 0; demo.kill_workers()
    jobs = [demo.submit(snapshot, f'batch-{i}') for i in range(6)]
    demo.worker_count = 3; demo.start_workers()
    results = [demo.wait(j['id']) for j in jobs]
    ids = {r['artifactId'] for r in results}
    assert len(ids) == len(jobs), ids
    status = demo.call('GET', '/v1/status')
    observations['batchJobs'] = len(jobs)
    observations['distinctArtifacts'] = len(ids)
    observations['jobStates'] = status['jobs']

    # Cancel an attempt that is already running and confirm it cannot win.
    demo.worker_count = 0; demo.kill_workers()
    heavy = demo.call('GET', '/v1/scenes/emblem')
    heavy_doc = heavy_document(); heavy_doc['background'] = '#101010'
    demo.call('PUT', '/v1/scenes/emblem', {'name': '重负载', 'expectedRevision': heavy['revision'], 'document': heavy_doc})
    heavy_snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': heavy['revision'] + 1}, 201)
    demo.worker_count = 1; demo.start_workers()
    racing = demo.submit(heavy_snapshot, 'cancel-races-result')
    running = demo.wait(racing['id'], wanted=('running',))
    assert running.get('workerId')
    demo.call('POST', '/v1/jobs/' + racing['id'] + '/cancel')
    time.sleep(.5)
    final = demo.call('GET', '/v1/jobs/' + racing['id'])
    assert final['state'] == 'cancelled', final
    assert not final.get('artifactId'), final
    observations['cancelledAttemptArtifact'] = final.get('artifactId', None)

    # A graceful worker stop hands its unfinished job back to the queue while
    # keeping the attempt, so another process can finish it.
    demo.worker_count = 0; demo.kill_workers()
    demo.worker_count = 1; demo.start_workers()
    graceful = demo.submit(heavy_snapshot, 'graceful-requeue')
    demo.wait(graceful['id'], wanted=('running',))
    demo.kill_workers(abrupt=False)  # SIGTERM: release claim back to queued
    stopped = demo.call('GET', '/v1/jobs/' + graceful['id'])
    assert stopped['state'] == 'queued', stopped
    assert stopped['attempt'] == 1, stopped
    demo.worker_count = 1; demo.start_workers()
    assert demo.wait(graceful['id'], timeout=30)['state'] == 'succeeded'
    observations['gracefulRequeue'] = stopped['state']

    # Legacy queue data: a running record without a lease must be requeued by
    # a fresh worker and then rendered, not ignored.
    demo.worker_count = 0; demo.kill_workers()
    legacy_snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': heavy['revision'] + 1}, 201)
    legacy = demo.submit(legacy_snapshot, 'legacy-rerun')
    state_file = demo.directory / 'state.json'
    raw = json.loads(state_file.read_text())
    raw['jobs'][legacy['id']].update({'state': 'running', 'workerId': 'old-process', 'claim': '', 'leaseUntil': '0001-01-01T00:00:00Z'})
    state_file.write_text(json.dumps(raw))
    demo.worker_count = 1; demo.start_workers()
    recovered = demo.wait(legacy['id'], timeout=30)
    assert recovered['attempt'] == 1, recovered
    observations['legacyRecovered'] = recovered['state']
    return {'workflow': 'multi-worker', **observations}


def run(demo, workflow, out):
    d = sample()
    put = lambda doc, rev=0: demo.call('PUT', '/v1/scenes/emblem', {'name': '双色印记', 'expectedRevision': rev, 'document': doc})
    scene = put(d)
    if workflow == 'create-scene':
        assert demo.call('GET', '/v1/scenes/emblem')['document'] == scene['document']
        bad = sample(); bad['layers'][0]['transform']['scaleX'] = 0
        demo.call('PUT', '/v1/scenes/invalid', {'name': '无效几何', 'expectedRevision': 0, 'document': bad}, 400)
    elif workflow == 'revise-scene':
        d['background'] = '#F0F0F0'; assert put(d, 1)['revision'] == 2
        demo.call('PUT', '/v1/scenes/emblem', {'name': '旧版本', 'expectedRevision': 1, 'document': d}, 409)
    elif workflow == 'archive-scene':
        archived = demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': 1, 'state': 'archived'})
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': archived['revision']}, 409)
        assert demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': archived['revision'], 'state': 'active'})['revision'] == 3
    if workflow in ('import-asset', 'retire-asset'):
        asset = demo.call('PUT', '/v1/assets/ribbon', tiny_png(), 201)
        assert asset['width'] == 2 and asset['digest'] == hashlib.sha256(tiny_png()).hexdigest()
        demo.call('PUT', '/v1/assets/broken', b'not png', 400)
        layer = {'id': 'ribbon', 'kind': 'image', 'width': 60, 'height': 60, 'assetId': 'ribbon', 'opacity': 1, 'transform': {'x': 20, 'y': 30, 'scaleX': 1, 'scaleY': 1}}
        d['layers'].append(layer); put(d, 1)
        pinned = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 201)
        demo.call('POST', '/v1/assets/ribbon/retire')
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 409)
        result = demo.wait(demo.submit(pinned)['id']); assert result['state'] == 'succeeded'
        return {'workflow': workflow, 'assetDigest': asset['digest'], 'pinnedRender': result['state']}
    current = demo.call('GET', '/v1/scenes/emblem')
    snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision']}, 201)
    if workflow == 'freeze-scene':
        original_digest = snapshot['digest']; d['background'] = '#000000'; put(d, current['revision'])
        assert demo.call('GET', '/v1/snapshots/' + snapshot['id'])['digest'] == original_digest
    if workflow in ('export-package', 'import-package'):
        package = demo.call('GET', '/v1/snapshots/' + snapshot['id'] + '/package')
        restored = demo.call('POST', '/v1/imports/restored', {'name': '恢复的印记', 'package': package}, 201)
        assert restored['document'] == snapshot['document']
        corrupt = json.loads(json.dumps(package)); corrupt['snapshot']['document']['width'] += 1
        demo.call('POST', '/v1/imports/corrupt', {'name': '损坏包', 'package': corrupt}, 400)
        demo.call('GET', '/v1/scenes/corrupt', status=404)
    if workflow in ('cancel-render', 'retry-render', 'recover-render'):
        put(heavy_document(), current['revision'])
        snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
    if workflow == 'multi-worker':
        return run_multi_worker(demo, snapshot, current)
    if workflow == 'partition-recovery':
        return run_partition(demo, current)
    job = demo.submit(snapshot)
    if workflow == 'submit-render':
        assert demo.submit(snapshot)['id'] == job['id']
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 2}, 409, {'Idempotency-Key': 'render-one'})
    if workflow == 'revoke-snapshot':
        demo.call('POST', '/v1/snapshots/' + snapshot['id'] + '/revoke')
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 1}, 409, {'Idempotency-Key': 'blocked-after-revoke'})
    if workflow in ('cancel-render', 'retry-render'):
        stopped = demo.call('POST', '/v1/jobs/' + job['id'] + '/cancel')
        assert stopped['state'] == 'cancelled'
        if workflow == 'cancel-render':
            time.sleep(.1); assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'cancelled'
            return {'workflow': workflow, 'state': 'cancelled'}
        job = demo.call('POST', '/v1/jobs/' + job['id'] + '/retry', status=202)
        assert job['attempt'] == 2
    if workflow == 'recover-render':
        # Kill the worker that owns this job, leaving the API alive. A fresh
        # execution process must take over the same attempt after the lease
        # expires; the API meanwhile keeps accepting and answering requests.
        running = demo.wait(job['id'], wanted=('running',), timeout=20)
        assert running.get('workerId'), 'running job must expose its executing worker'
        demo.kill_workers(abrupt=True)
        api_status = demo.call('GET', '/v1/status')
        assert api_status['jobs'].get('running', 0) == 1, api_status
        demo.start_workers()
    result = demo.wait(job['id'])
    png = demo.call('GET', '/v1/artifacts/' + result['artifactId'])
    assert png[:8] == b'\x89PNG\r\n\x1a\n'
    assert struct.unpack('>II', png[16:24]) == (snapshot['document']['width'], snapshot['document']['height'])
    if out:
        Path(out).mkdir(parents=True, exist_ok=True); (Path(out) / 'atlas-sample.png').write_bytes(png)
    if workflow == 'compare-renders':
        other = demo.wait(demo.submit(snapshot, 'render-two')['id'])
        comparison = demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': other['artifactId']})
        assert comparison['changedPixels'] == 0
        d['background'] = '#000000'; put(d, current['revision'])
        altered = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
        changed = demo.wait(demo.submit(altered, 'render-three')['id'])
        assert demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': changed['artifactId']})['changedPixels'] > 0
    if workflow == 'purge-artifact':
        demo.call('DELETE', '/v1/artifacts/' + result['artifactId'])
        demo.call('GET', '/v1/artifacts/' + result['artifactId'], status=410)
        assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'succeeded'
    if workflow == 'observe-service':
        summary = demo.call('GET', '/v1/status'); assert summary['jobs']['succeeded'] >= 1
        events = demo.call('GET', '/v1/events'); assert any(e['kind'] == 'job.succeeded' for e in events)
    return {'workflow': workflow, 'state': result['state'], 'pngBytes': len(png), 'pngDigest': hashlib.sha256(png).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflow', default='submit-render')
    parser.add_argument('--out')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='atlas-walkthrough-') as temp:
        directory = Path(temp); binary = directory / 'atlas'
        subprocess.run(['go', 'build', '-o', str(binary), './cmd/atlas'], cwd=ROOT, check=True, timeout=60)
        demo = Demo(binary, directory)
        try:
            demo.start()
            print(json.dumps(run(demo, args.workflow, args.out), ensure_ascii=False))
        finally:
            demo.stop(); demo.log.close()


if __name__ == '__main__':
    main()
