import { useEffect, useMemo, useState } from 'react';
import { Activity, AlertTriangle, Cable, Database, Gauge, Layers } from 'lucide-react';
import { Button } from './components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './components/ui/card';
import { Input } from './components/ui/input';
import { Textarea } from './components/ui/textarea';
import { Badge } from './components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './components/ui/table';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './components/ui/select';
import { Switch } from './components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs';

const BASE_KEY = 'thorstream-ui-base';
const POLL_KEY = 'thorstream-ui-poll';

function asNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function parseLabels(raw) {
  return raw.split(',').reduce((acc, item) => {
    const [key, val] = item.split('=');
    if (key && val) acc[key.trim()] = val.trim().replace(/^"|"$/g, '');
    return acc;
  }, {});
}

function parseProm(text) {
  const out = {};
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const b0 = trimmed.indexOf('{');
    if (b0 >= 0) {
      const b1 = trimmed.indexOf('}');
      const name = trimmed.slice(0, b0);
      const labels = parseLabels(trimmed.slice(b0 + 1, b1));
      const value = asNumber(trimmed.slice(b1 + 1).trim());
      out[name] ||= [];
      out[name].push({ labels, value });
    } else {
      const [name, rawValue] = trimmed.split(/\s+/);
      out[name] ||= [];
      out[name].push({ labels: {}, value: asNumber(rawValue) });
    }
  }
  return out;
}

function firstMetric(map, key) {
  return map[key]?.[0]?.value ?? 0;
}

export function App() {
  const [baseUrl, setBaseUrl] = useState(localStorage.getItem(BASE_KEY) || '/api');
  const [status, setStatus] = useState('Idle');
  const [error, setError] = useState('');
  const [autoRefresh, setAutoRefresh] = useState(localStorage.getItem(POLL_KEY) !== 'off');
  const [metricsRaw, setMetricsRaw] = useState('');
  const [metrics, setMetrics] = useState({});
  const [connectors, setConnectors] = useState([]);
  const [plugins, setPlugins] = useState([]);
  const [subjects, setSubjects] = useState([]);

  const [connectorName, setConnectorName] = useState('');
  const [connectorClass, setConnectorClass] = useState('io.confluent.connect.s3.S3SinkConnector');
  const [connectorTopics, setConnectorTopics] = useState('events');

  const [schemaSubject, setSchemaSubject] = useState('events-value');
  const [schemaType, setSchemaType] = useState('AVRO');
  const [schemaText, setSchemaText] = useState('{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}');
  const [compatibilityResult, setCompatibilityResult] = useState('');

  const summary = useMemo(() => {
    const urp = firstMetric(metrics, 'thorstream_under_replicated_partitions');
    const p99 = firstMetric(metrics, 'thorstream_request_latency_p99_ms');
    const produce = firstMetric(metrics, 'thorstream_produce_records_total');
    const fetch = firstMetric(metrics, 'thorstream_fetch_records_total');
    const bytesIn = firstMetric(metrics, 'thorstream_produce_bytes_total');
    const bytesOut = firstMetric(metrics, 'thorstream_fetch_bytes_total');
    const lagRows = metrics['thorstream_consumer_lag'] || [];
    const lagTotal = lagRows.reduce((acc, row) => acc + row.value, 0);
    return { urp, p99, produce, fetch, bytesIn, bytesOut, lagTotal };
  }, [metrics]);

  const alerts = useMemo(() => {
    const out = [];
    if (summary.urp > 0) out.push(`Under-replicated partitions: ${summary.urp}`);
    if (summary.p99 > 200) out.push(`High request latency p99: ${summary.p99} ms`);
    if (summary.lagTotal > 0) out.push(`Consumer lag total: ${summary.lagTotal}`);
    if (out.length === 0) out.push('No active alerts');
    return out;
  }, [summary]);

  async function api(path, options = {}) {
    const base = baseUrl.trim().replace(/\/$/, '');
    const res = await fetch(`${base}${path}`, {
      headers: {
        'content-type': 'application/json',
        ...(options.headers || {}),
      },
      ...options,
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`${res.status} ${res.statusText}: ${text.slice(0, 180)}`);
    }
    return res;
  }

  async function refreshAll() {
    try {
      setError('');
      setStatus('Refreshing…');

      const [metricsRes, connectorsRes, subjectsRes, pluginsRes] = await Promise.all([
        api('/metrics', { headers: {} }),
        api('/connectors'),
        api('/subjects'),
        api('/connector-plugins'),
      ]);

      const raw = await metricsRes.text();
      setMetricsRaw(raw);
      setMetrics(parseProm(raw));
      setConnectors(await connectorsRes.json());
      setSubjects(await subjectsRes.json());
      setPlugins(await pluginsRes.json());
      setStatus(`Updated ${new Date().toLocaleTimeString()}`);
    } catch (e) {
      setError(e.message);
      setStatus('Refresh failed');
    }
  }

  useEffect(() => {
    refreshAll();
  }, []);

  useEffect(() => {
    if (!autoRefresh) return;
    const timer = setInterval(() => {
      refreshAll();
    }, 5000);
    return () => clearInterval(timer);
  }, [autoRefresh, baseUrl]);

  function saveBase() {
    localStorage.setItem(BASE_KEY, baseUrl);
    setStatus('API base saved');
  }

  function toggleAutoRefresh(checked) {
    setAutoRefresh(checked);
    localStorage.setItem(POLL_KEY, checked ? 'on' : 'off');
  }

  async function createConnector(event) {
    event.preventDefault();
    try {
      await api('/connectors', {
        method: 'POST',
        body: JSON.stringify({
          name: connectorName,
          config: {
            'connector.class': connectorClass,
            'topics': connectorTopics,
            'tasks.max': '1',
          },
        }),
      });
      setConnectorName('');
      await refreshAll();
    } catch (e) {
      setError(e.message);
    }
  }

  async function connectorAction(name, action) {
    try {
      const methods = {
        pause: { method: 'PUT', path: `/connectors/${encodeURIComponent(name)}/pause` },
        resume: { method: 'PUT', path: `/connectors/${encodeURIComponent(name)}/resume` },
        delete: { method: 'DELETE', path: `/connectors/${encodeURIComponent(name)}` },
      };
      const target = methods[action];
      await api(target.path, { method: target.method });
      await refreshAll();
    } catch (e) {
      setError(e.message);
    }
  }

  async function registerSchema(event) {
    event.preventDefault();
    try {
      await api(`/subjects/${encodeURIComponent(schemaSubject)}/versions`, {
        method: 'POST',
        body: JSON.stringify({ schema: schemaText, schemaType }),
      });
      await refreshAll();
    } catch (e) {
      setError(e.message);
    }
  }

  async function checkCompatibility() {
    try {
      const res = await api(`/compatibility/subjects/${encodeURIComponent(schemaSubject)}/versions/latest`, {
        method: 'POST',
        body: JSON.stringify({ schema: schemaText, schemaType }),
      });
      const value = await res.json();
      setCompatibilityResult(value.is_compatible ? 'Compatible' : 'Incompatible');
    } catch (e) {
      setError(e.message);
    }
  }

  const lagRows = metrics['thorstream_consumer_lag'] || [];
  const sizeRows = metrics['thorstream_partition_size_bytes'] || [];

  return (
    <main className="mx-auto max-w-7xl space-y-4 p-4 md:p-6">
      <Card>
        <CardHeader>
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <CardTitle className="text-2xl">Thorstream Dashboard</CardTitle>
              <CardDescription>React + shadcn-style UI for streaming operations.</CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant={error ? 'destructive' : 'secondary'}>{error ? 'Degraded' : 'Healthy'}</Badge>
              <Button variant="outline" onClick={refreshAll}>Refresh now</Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-3 md:grid-cols-[1fr_auto_auto]">
            <Input value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} placeholder="/api or http://127.0.0.1:8083" />
            <Button variant="outline" onClick={saveBase}>Save API Base</Button>
            <label className="flex items-center gap-2 rounded-md border border-border px-3 text-sm">
              Auto refresh
              <Switch checked={autoRefresh} onCheckedChange={toggleAutoRefresh} />
            </label>
          </div>
          <p className="text-sm text-muted-foreground">{status}{error ? ` — ${error}` : ''}</p>
        </CardContent>
      </Card>

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        <MetricCard icon={Gauge} title="URP" value={summary.urp} />
        <MetricCard icon={Activity} title="Latency p99 ms" value={summary.p99} />
        <MetricCard icon={Layers} title="Lag total" value={summary.lagTotal} />
        <MetricCard icon={Database} title="Produced records" value={summary.produce} />
        <MetricCard icon={Database} title="Fetched records" value={summary.fetch} />
        <MetricCard icon={Cable} title="Bytes in / out" value={`${summary.bytesIn} / ${summary.bytesOut}`} />
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Alerts</CardTitle>
          <CardDescription>Common streaming platform health checks.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-2">
          {alerts.map((alert) => (
            <div key={alert} className="flex items-center gap-2 text-sm">
              <AlertTriangle className="h-4 w-4 text-yellow-400" />
              <span>{alert}</span>
            </div>
          ))}
        </CardContent>
      </Card>

      <Tabs defaultValue="ops">
        <TabsList>
          <TabsTrigger value="ops">Ops</TabsTrigger>
          <TabsTrigger value="connect">Connect</TabsTrigger>
          <TabsTrigger value="schema">Schema</TabsTrigger>
          <TabsTrigger value="raw">Raw Metrics</TabsTrigger>
        </TabsList>

        <TabsContent value="ops" className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Consumer Lag</CardTitle>
              <CardDescription>Per group/topic/partition lag view.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Group</TableHead>
                    <TableHead>Topic</TableHead>
                    <TableHead>Partition</TableHead>
                    <TableHead>Lag</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {lagRows.length === 0 ? (
                    <TableRow><TableCell colSpan={4} className="text-muted-foreground">No lag metrics.</TableCell></TableRow>
                  ) : lagRows.map((row, idx) => (
                    <TableRow key={`${row.labels.group}-${row.labels.topic}-${row.labels.partition}-${idx}`}>
                      <TableCell>{row.labels.group || '-'}</TableCell>
                      <TableCell>{row.labels.topic || '-'}</TableCell>
                      <TableCell>{row.labels.partition || '-'}</TableCell>
                      <TableCell>{row.value}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Partition Size</CardTitle>
              <CardDescription>Storage pressure by topic/partition.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Topic</TableHead>
                    <TableHead>Partition</TableHead>
                    <TableHead>Bytes</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sizeRows.length === 0 ? (
                    <TableRow><TableCell colSpan={3} className="text-muted-foreground">No partition size metrics.</TableCell></TableRow>
                  ) : sizeRows.map((row, idx) => (
                    <TableRow key={`${row.labels.topic}-${row.labels.partition}-${idx}`}>
                      <TableCell>{row.labels.topic || '-'}</TableCell>
                      <TableCell>{row.labels.partition || '-'}</TableCell>
                      <TableCell>{row.value}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="connect" className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Create Connector</CardTitle>
              <CardDescription>Common connector lifecycle operation.</CardDescription>
            </CardHeader>
            <CardContent>
              <form className="space-y-2" onSubmit={createConnector}>
                <Input value={connectorName} onChange={(e) => setConnectorName(e.target.value)} placeholder="connector name" required />
                <Select value={connectorClass} onValueChange={setConnectorClass}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="io.confluent.connect.s3.S3SinkConnector">S3 Sink</SelectItem>
                    <SelectItem value="io.confluent.connect.jdbc.JdbcSinkConnector">JDBC Sink</SelectItem>
                    <SelectItem value="io.debezium.connector.postgresql.PostgresConnector">Debezium Postgres</SelectItem>
                  </SelectContent>
                </Select>
                <Input value={connectorTopics} onChange={(e) => setConnectorTopics(e.target.value)} placeholder="topics: events,orders" required />
                <Button type="submit">Create</Button>
              </form>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Connector Inventory</CardTitle>
              <CardDescription>List/pause/resume/delete active connectors.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-2">
              {connectors.length === 0 ? (
                <p className="text-sm text-muted-foreground">No connectors found.</p>
              ) : connectors.map((name) => (
                <div key={name} className="flex flex-wrap items-center gap-2 rounded-md border border-border p-2">
                  <span className="min-w-48 text-sm">{name}</span>
                  <Button size="sm" variant="outline" onClick={() => connectorAction(name, 'pause')}>Pause</Button>
                  <Button size="sm" variant="outline" onClick={() => connectorAction(name, 'resume')}>Resume</Button>
                  <Button size="sm" variant="destructive" onClick={() => connectorAction(name, 'delete')}>Delete</Button>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Connector Plugin Catalog</CardTitle>
              <CardDescription>Available plugin classes from compatibility API.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Class</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Version</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {plugins.map((plugin) => (
                    <TableRow key={plugin.class}>
                      <TableCell>{plugin.class}</TableCell>
                      <TableCell>{plugin.type}</TableCell>
                      <TableCell>{plugin.version}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="schema" className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Register Schema</CardTitle>
              <CardDescription>Schema version management for event contracts.</CardDescription>
            </CardHeader>
            <CardContent>
              <form className="space-y-2" onSubmit={registerSchema}>
                <Input value={schemaSubject} onChange={(e) => setSchemaSubject(e.target.value)} required />
                <Select value={schemaType} onValueChange={setSchemaType}>
                  <SelectTrigger><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="AVRO">AVRO</SelectItem>
                    <SelectItem value="JSON">JSON</SelectItem>
                    <SelectItem value="PROTOBUF">PROTOBUF</SelectItem>
                  </SelectContent>
                </Select>
                <Textarea rows={6} value={schemaText} onChange={(e) => setSchemaText(e.target.value)} required />
                <div className="flex flex-wrap gap-2">
                  <Button type="submit">Register</Button>
                  <Button type="button" variant="outline" onClick={checkCompatibility}>Check Compatibility</Button>
                  {compatibilityResult ? <Badge variant={compatibilityResult === 'Compatible' ? 'default' : 'destructive'}>{compatibilityResult}</Badge> : null}
                </div>
              </form>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Subjects</CardTitle>
              <CardDescription>Known schema subjects.</CardDescription>
            </CardHeader>
            <CardContent>
              {subjects.length === 0 ? (
                <p className="text-sm text-muted-foreground">No subjects found.</p>
              ) : (
                <ul className="space-y-1 text-sm">
                  {subjects.map((s) => <li key={s} className="rounded-md border border-border px-2 py-1">{s}</li>)}
                </ul>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="raw">
          <Card>
            <CardHeader>
              <CardTitle>Raw Prometheus</CardTitle>
              <CardDescription>Useful for copy/paste into monitoring workflows.</CardDescription>
            </CardHeader>
            <CardContent>
              <pre className="max-h-[440px] overflow-auto rounded-md border border-border bg-secondary p-3 text-xs">{metricsRaw || 'No metrics loaded.'}</pre>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </main>
  );
}

function MetricCard({ icon: Icon, title, value }) {
  return (
    <Card>
      <CardContent className="flex items-center justify-between p-4">
        <div>
          <p className="text-xs text-muted-foreground">{title}</p>
          <p className="text-xl font-semibold">{value}</p>
        </div>
        <Icon className="h-5 w-5 text-primary" />
      </CardContent>
    </Card>
  );
}
