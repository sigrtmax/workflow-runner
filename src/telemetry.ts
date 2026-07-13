import {
  ROOT_CONTEXT,
  SpanStatusCode,
  trace,
  type Context,
  type Span,
  defaultTextMapGetter,
  defaultTextMapSetter,
} from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import type { OnApplicationShutdown } from '@nestjs/common';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import {
  MeterProvider,
  PeriodicExportingMetricReader,
  type MetricReader,
} from '@opentelemetry/sdk-metrics';
import {
  BatchSpanProcessor,
  type SpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';

export type TraceCarrier = Record<string, string>;

export interface WorkflowSpan {
  carrier: TraceCarrier;
  end(outcome: string): void;
}

type StepAttributes = {
  workflowName: string;
  version: number;
  stepId: string;
  stepType: string;
  attempt: number;
  executionId: string;
};

export class Telemetry implements OnApplicationShutdown {
  private readonly tracerProvider: NodeTracerProvider;
  private readonly meterProvider: MeterProvider;
  private readonly tracer;
  private readonly stepDuration;
  private readonly stepErrors;

  constructor(
    options: {
      endpoint?: string;
      spanProcessor?: SpanProcessor;
      metricReader?: MetricReader;
    } = {},
  ) {
    const spanProcessors = options.spanProcessor
      ? [options.spanProcessor]
      : options.endpoint
        ? [
            new BatchSpanProcessor(
              new OTLPTraceExporter({
                url: signalEndpoint(options.endpoint, 'v1/traces'),
              }),
            ),
          ]
        : [];
    const metricReader = options.metricReader
      ? options.metricReader
      : options.endpoint
        ? new PeriodicExportingMetricReader({
            exporter: new OTLPMetricExporter({
              url: signalEndpoint(options.endpoint, 'v1/metrics'),
            }),
          })
        : undefined;

    this.tracerProvider = new NodeTracerProvider({ spanProcessors });
    this.meterProvider = new MeterProvider({
      readers: metricReader ? [metricReader] : [],
    });
    this.tracer = this.tracerProvider.getTracer('workflow-runner');
    const meter = this.meterProvider.getMeter('workflow-runner');
    this.stepDuration = meter.createHistogram('workflow.step.duration', {
      unit: 's',
    });
    this.stepErrors = meter.createCounter('workflow.step.errors');
  }

  startWorkflow(
    executionId: string,
    name: string,
    version: number,
  ): WorkflowSpan {
    const span = this.tracer.startSpan(
      'workflow.run',
      {
        attributes: {
          'workflow.execution_id': executionId,
          'workflow.name': name,
          'workflow.version': version,
        },
      },
      ROOT_CONTEXT,
    );
    return this.wrap(span);
  }

  startStep(
    carrier: TraceCarrier,
    attributes: StepAttributes,
    startedAt?: Date,
  ): WorkflowSpan {
    const span = this.tracer.startSpan(
      'workflow.step',
      {
        attributes: {
          'workflow.execution_id': attributes.executionId,
          'workflow.name': attributes.workflowName,
          'workflow.version': attributes.version,
          'workflow.step_id': attributes.stepId,
          'workflow.step_type': attributes.stepType,
          'workflow.attempt': attributes.attempt,
        },
        ...(startedAt ? { startTime: startedAt } : {}),
      },
      extractContext(carrier),
    );
    const elapsedBeforeThisProcess = startedAt
      ? Math.max(0, (Date.now() - startedAt.getTime()) / 1000)
      : 0;
    const monotonicStartedAt = performance.now();
    let ended = false;
    const metricAttributes = {
      workflowName: attributes.workflowName,
      version: attributes.version,
      stepId: attributes.stepId,
      stepType: attributes.stepType,
    };

    return {
      carrier: injectCarrier(span),
      end: (outcome) => {
        if (ended) return;
        ended = true;
        const labels = { ...metricAttributes, outcome };
        this.stepDuration.record(
          elapsedBeforeThisProcess +
            (performance.now() - monotonicStartedAt) / 1000,
          labels,
        );
        if (outcome !== 'succeeded') {
          this.stepErrors.add(1, labels);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: 'workflow step failed',
          });
        }
        span.end();
      },
    };
  }

  async shutdown(): Promise<void> {
    await this.flush();
    await Promise.all([
      this.tracerProvider.shutdown(),
      this.meterProvider.shutdown(),
    ]);
  }

  async onApplicationShutdown(): Promise<void> {
    await this.shutdown();
  }

  async flush(): Promise<void> {
    await Promise.all([
      this.tracerProvider.forceFlush(),
      this.meterProvider.forceFlush(),
    ]);
  }

  private wrap(span: Span): WorkflowSpan {
    let ended = false;
    return {
      carrier: injectCarrier(span),
      end: (outcome) => {
        if (ended) return;
        ended = true;
        if (
          outcome !== 'accepted' &&
          outcome !== 'succeeded' &&
          outcome !== 'duplicate'
        ) {
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: 'workflow admission failed',
          });
        }
        span.end();
      },
    };
  }
}

function injectCarrier(span: Span): TraceCarrier {
  const carrier: TraceCarrier = {};
  propagator.inject(
    trace.setSpan(ROOT_CONTEXT, span),
    carrier,
    defaultTextMapSetter,
  );
  return carrier;
}

function extractContext(carrier: TraceCarrier): Context {
  return propagator.extract(ROOT_CONTEXT, carrier, defaultTextMapGetter);
}

const propagator = new W3CTraceContextPropagator();

function signalEndpoint(
  endpoint: string,
  signal: 'v1/traces' | 'v1/metrics',
): string {
  return new URL(
    signal,
    endpoint.endsWith('/') ? endpoint : `${endpoint}/`,
  ).toString();
}
