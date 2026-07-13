import {
  AggregationTemporality,
  InMemoryMetricExporter,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import { Telemetry } from '../src/telemetry';

const execFileAsync = promisify(execFile);

describe('Telemetry', () => {
  it('does not report an idempotent replay as an admission error', async () => {
    const exporter = new InMemorySpanExporter();
    const telemetry = new Telemetry({
      spanProcessor: new SimpleSpanProcessor(exporter),
    });
    telemetry.startWorkflow('duplicate', 'leads', 1).end('duplicate');
    await telemetry.flush();
    expect(exporter.getFinishedSpans()[0]!.status.code).toBe(0);
    await telemetry.shutdown();
  });

  it('posts traces and metrics to standard OTLP signal paths from a base endpoint', async () => {
    const script = `
      const { createServer } = require('node:http');
      const { Telemetry } = require('./src/telemetry');
      const paths = [];
      const server = createServer((request, response) => {
        paths.push(request.url);
        request.resume();
        response.writeHead(200).end();
      });
      server.listen(0, '127.0.0.1', async () => {
        try {
          const address = server.address();
          const telemetry = new Telemetry({ endpoint: 'http://127.0.0.1:' + address.port });
          const workflow = telemetry.startWorkflow('execution-otlp', 'crm-sync', 3);
          workflow.end('accepted');
          const step = telemetry.startStep(workflow.carrier, {
            workflowName: 'crm-sync', version: 3, stepId: 'send-crm', stepType: 'http', attempt: 1, executionId: 'execution-otlp'
          });
          step.end('succeeded');
          await telemetry.flush();
          await telemetry.shutdown();
          server.close(() => process.stdout.write(JSON.stringify(paths)));
        } catch (error) {
          server.close(() => { process.stderr.write(String(error)); process.exitCode = 1; });
        }
      });
    `;
    const { stdout } = await execFileAsync(
      process.execPath,
      ['-r', 'ts-node/register', '-e', script],
      { cwd: process.cwd() },
    );
    const receivedPaths = JSON.parse(stdout) as string[];

    expect(receivedPaths).toEqual(
      expect.arrayContaining(['/v1/traces', '/v1/metrics']),
    );
  });

  it('uses the W3C propagator for valid trace context and ignores the reserved ff version', async () => {
    const exporter = new InMemorySpanExporter();
    const telemetry = new Telemetry({
      spanProcessor: new SimpleSpanProcessor(exporter),
    });
    const carrier = {
      traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01',
      tracestate: 'vendor=value',
    };

    const propagated = telemetry.startStep(carrier, {
      workflowName: 'crm-sync',
      version: 3,
      stepId: 'send-crm',
      stepType: 'http',
      attempt: 1,
      executionId: 'execution-42',
    });
    propagated.end('succeeded');
    const invalidVersion = telemetry.startStep(
      { ...carrier, traceparent: carrier.traceparent.replace(/^00/, 'ff') },
      {
        workflowName: 'crm-sync',
        version: 3,
        stepId: 'send-crm',
        stepType: 'http',
        attempt: 2,
        executionId: 'execution-42',
      },
    );
    invalidVersion.end('succeeded');
    await telemetry.flush();

    expect(propagated.carrier.traceparent).toContain(
      '0123456789abcdef0123456789abcdef',
    );
    expect(propagated.carrier.tracestate).toBe('vendor=value');
    expect(invalidVersion.carrier.traceparent).not.toContain(
      '0123456789abcdef0123456789abcdef',
    );
    await telemetry.shutdown();
  });

  it('uses a persisted step start time for the span and duration metric', async () => {
    const metricExporter = new InMemoryMetricExporter(
      AggregationTemporality.CUMULATIVE,
    );
    const telemetry = new Telemetry({
      metricReader: new PeriodicExportingMetricReader({
        exporter: metricExporter,
        exportIntervalMillis: 60_000,
      }),
    });
    const persistedStart = new Date(Date.now() - 2_000);
    const step = telemetry.startStep(
      {
        traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01',
      },
      {
        workflowName: 'crm-sync',
        version: 3,
        stepId: 'wait-for-crm',
        stepType: 'delay',
        attempt: 1,
        executionId: 'execution-delay',
      },
      persistedStart,
    );
    step.end('succeeded');
    await telemetry.flush();

    const duration = metricExporter
      .getMetrics()
      .flatMap((resource) =>
        resource.scopeMetrics.flatMap((scope) => scope.metrics),
      )
      .find((metric) => metric.descriptor.name === 'workflow.step.duration');
    const point = duration?.dataPoints[0];
    expect(point?.value).toMatchObject({ count: 1 });
    expect((point?.value as { sum: number }).sum).toBeGreaterThanOrEqual(2);
    await telemetry.shutdown();
  });

  it('continues a persisted workflow trace in a fresh provider and exports bounded retry metrics', async () => {
    const spanExporter = new InMemorySpanExporter();
    const spanProcessor = new SimpleSpanProcessor(spanExporter);
    const metricExporter = new InMemoryMetricExporter(
      AggregationTemporality.CUMULATIVE,
    );
    const metricReader = new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 60_000,
    });
    const telemetry = new Telemetry({ spanProcessor });

    const workflow = telemetry.startWorkflow('execution-42', 'crm-sync', 3);
    workflow.end('accepted');
    await telemetry.flush();

    const restoredSpanExporter = new InMemorySpanExporter();
    const restoredTelemetry = new Telemetry({
      spanProcessor: new SimpleSpanProcessor(restoredSpanExporter),
      metricReader,
    });
    const failedAttempt = restoredTelemetry.startStep(workflow.carrier, {
      workflowName: 'crm-sync',
      version: 3,
      stepId: 'send-crm',
      stepType: 'http',
      attempt: 1,
      executionId: 'execution-42',
    });
    failedAttempt.end('temporary_failure');

    const successfulAttempt = restoredTelemetry.startStep(workflow.carrier, {
      workflowName: 'crm-sync',
      version: 3,
      stepId: 'send-crm',
      stepType: 'http',
      attempt: 2,
      executionId: 'execution-42',
    });
    successfulAttempt.end('succeeded');

    await restoredTelemetry.flush();

    const root = spanExporter
      .getFinishedSpans()
      .find((span) => span.name === 'workflow.run');
    const steps = restoredSpanExporter
      .getFinishedSpans()
      .filter((span) => span.name === 'workflow.step');
    expect(root).toBeDefined();
    expect(steps).toHaveLength(2);
    expect(
      steps.every(
        (span) => span.spanContext().traceId === root?.spanContext().traceId,
      ),
    ).toBe(true);
    expect(steps[0]?.status.code).toBe(2);
    expect(steps[0]?.status.message).toBe('workflow step failed');

    const metrics = metricExporter
      .getMetrics()
      .flatMap((resource) =>
        resource.scopeMetrics.flatMap((scope) => scope.metrics),
      );
    const duration = metrics.find(
      (metric) => metric.descriptor.name === 'workflow.step.duration',
    );
    const errors = metrics.find(
      (metric) => metric.descriptor.name === 'workflow.step.errors',
    );
    expect(duration?.dataPoints).toHaveLength(2);
    expect(errors?.dataPoints).toHaveLength(1);
    expect(errors?.dataPoints[0]?.attributes).toEqual({
      workflowName: 'crm-sync',
      version: 3,
      stepId: 'send-crm',
      stepType: 'http',
      outcome: 'temporary_failure',
    });
    expect(errors?.dataPoints[0]?.attributes).not.toHaveProperty('executionId');
    expect(errors?.dataPoints[0]?.attributes).not.toHaveProperty('attempt');

    await Promise.all([telemetry.shutdown(), restoredTelemetry.shutdown()]);
  });
});
