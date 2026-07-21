import { createServer, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { once } from 'node:events';

export interface IntegrationCall {
  path: string;
  key: string;
  traceparent: string;
  body: unknown;
}

export class MockIntegrations {
  readonly calls: IntegrationCall[] = [];
  readonly effects = new Map<string, unknown>();
  readonly held = new Map<string, ServerResponse[]>();
  readonly statuses = new Map<string, number>();
  readonly bodies = new Map<string, unknown>();
  maxActive = 0;
  private active = 0;
  private readonly server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => {
      const path = request.url!;
      const key = String(request.headers['idempotency-key'] ?? '');
      const text = Buffer.concat(chunks).toString();
      const body: unknown = text ? JSON.parse(text) : null;
      this.calls.push({
        path,
        key,
        body,
        traceparent: String(request.headers.traceparent ?? ''),
      });
      this.active += 1;
      this.maxActive = Math.max(this.active, this.maxActive);
      response.once('close', () => {
        this.active -= 1;
      });
      const status = this.statuses.get(path) ?? 201;
      if (status === 201) {
        // The receiver applies a side effect once even if its reply is lost.
        if (!this.effects.has(key)) this.effects.set(key, body);
      }
      const held = this.held.get(path);
      if (held) held.push(response);
      else this.reply(response, status, this.bodies.get(path));
    });
  });

  async listen(): Promise<string> {
    this.server.listen(0, '127.0.0.1');
    await once(this.server, 'listening');
    return `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
  }
  hold(path: string): void {
    this.held.set(path, []);
  }
  release(path: string): void {
    const replies = this.held.get(path) ?? [];
    this.held.delete(path);
    for (const response of replies)
      this.reply(
        response,
        this.statuses.get(path) ?? 201,
        this.bodies.get(path),
      );
  }
  async close(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      this.server.close((error) => (error ? reject(error) : resolve())),
    );
  }
  private reply(
    response: ServerResponse,
    status: number,
    body?: unknown,
  ): void {
    response.writeHead(status, { 'Content-Type': 'application/json' });
    response.end(
      JSON.stringify(
        body === undefined
          ? { id: 'synthetic-lead', accepted: status === 201 }
          : body,
      ),
    );
  }
}
