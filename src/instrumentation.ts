/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  diag,
  propagation,
  trace,
  context,
  SpanKind,
  SpanStatusCode,
  Context,
  Span,
  ROOT_CONTEXT,
} from "@opentelemetry/api";
import {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition,
  isWrapped,
} from "@opentelemetry/instrumentation";
import { SemanticAttributes } from "@opentelemetry/semantic-conventions";
import type * as Nats from "nats";
import { NatsInstrumentationConfig } from "./types";
import * as utils from "./utils";
import { VERSION } from "./version";

type Message = Nats.Msg | Nats.JsMsg;
type SubscriptionLike = Nats.Subscription | { [key: string]: unknown };
type AsyncMessageIterator = AsyncIterableIterator<Message>;

interface NatsHelpers {
  /** Tracks which subjects are reply addresses */
  replySubjects: Set<string>;
  headers?: () => Nats.MsgHdrs;
}

/**
 * Nats instrumentation for Opentelemetry
 */
export class NatsInstrumentation extends InstrumentationBase<typeof Nats> {
  constructor(protected override _config: NatsInstrumentationConfig = {}) {
    super("@opentelemetry/instrumentation-nats", VERSION, _config);
    this._natsHelpers = {
      replySubjects: new Set(),
    };
  }

  private readonly _natsHelpers: NatsHelpers;
  private readonly _connectionWrappers = new WeakMap<
    Nats.NatsConnection,
    Map<PropertyKey, unknown>
  >();
  private readonly _jetStreamWrappers = new WeakMap<
    object,
    Map<PropertyKey, unknown>
  >();
  private readonly _messageContexts = new WeakMap<object, Context>();

  init(): InstrumentationNodeModuleDefinition<typeof Nats> {
    return new InstrumentationNodeModuleDefinition<typeof Nats>(
      "nats",
      ["2.*"],
      (moduleExports, moduleVersion) => {
        diag.debug(`Applying nats patch for nats@${moduleVersion}`);
        const { headers } = moduleExports as typeof Nats;
        this._natsHelpers.headers = headers;
        this.ensureWrapped(
          moduleVersion,
          moduleExports,
          "connect",
          this.wrapConnect.bind(this)
        );
        return moduleExports;
      },
      (moduleExports, moduleVersion) => {
        if (moduleExports === undefined) return;
        this._unwrap(moduleExports, "connect");
        diag.debug(`Removing nats patch for nats@${moduleVersion}`);
      }
    );
  }

  private wrapConnect(originalFunc: typeof import("nats").connect) {
    const instrumentation = this;
    return async function connect(
      this: unknown,
      opts?: Nats.ConnectionOptions
    ): Promise<Nats.NatsConnection> {
      const nc = await originalFunc.call(this, opts);
      return instrumentation.createInstrumentedConnection(nc);
    };
  }

  private createInstrumentedConnection(
    nc: Nats.NatsConnection
  ): Nats.NatsConnection {
    const instrumentation = this;
    return new Proxy(nc, {
      get(target, prop, receiver) {
        switch (prop) {
          case "publish":
            return instrumentation.getConnectionWrapper(target, prop, () =>
              instrumentation.createPublishWrapper(target, target.publish)
            );
          case "request":
            return instrumentation.getConnectionWrapper(target, prop, () =>
              instrumentation.createRequestWrapper(target, target.request)
            );
          case "subscribe":
            return instrumentation.getConnectionWrapper(target, prop, () =>
              instrumentation.createSubscribeWrapper(target, target.subscribe)
            );
          case "jetstream":
            return instrumentation.getConnectionWrapper(target, prop, () => {
              const original = target.jetstream;
              if (typeof original !== "function") {
                return original;
              }
              return function jetstream(this: Nats.NatsConnection, ...args: any[]) {
                const jsClient = original.apply(this, args);
                return instrumentation.wrapJetStream(target, jsClient);
              };
            });
          default:
            const value = Reflect.get(target, prop, receiver);
            if (typeof value === "function") {
              return value.bind(target);
            }
            return value;
        }
      },
    });
  }

  private getConnectionWrapper<T>(
    nc: Nats.NatsConnection,
    prop: PropertyKey,
    factory: () => T
  ): T {
    let cache = this._connectionWrappers.get(nc);
    if (!cache) {
      cache = new Map();
      this._connectionWrappers.set(nc, cache);
    }
    if (!cache.has(prop)) {
      cache.set(prop, factory());
    }
    return cache.get(prop) as T;
  }

  private getJetStreamWrapper<T>(
    client: object,
    prop: PropertyKey,
    factory: () => T
  ): T {
    let cache = this._jetStreamWrappers.get(client);
    if (!cache) {
      cache = new Map();
      this._jetStreamWrappers.set(client, cache);
    }
    if (!cache.has(prop)) {
      cache.set(prop, factory());
    }
    return cache.get(prop) as T;
  }

  private createPublishWrapper(
    nc: Nats.NatsConnection,
    original: Nats.NatsConnection["publish"]
  ) {
    const instrumentation = this;
    return function publish(
      this: Nats.NatsConnection,
      subject: string,
      data?: Uint8Array,
      options?: Nats.PublishOptions
    ): void {
      const publishOptions = options ? { ...options } : undefined;
      const reply = publishOptions?.reply;
      const { span, context: spanContext, headers } =
        instrumentation.startProducerSpan(nc, subject, data, reply, publishOptions);

      try {
        context.with(spanContext, () => {
          const finalOptions = headers
            ? { ...(publishOptions ?? {}), headers }
            : publishOptions;
          return original.call(this, subject, data, finalOptions);
        });
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        instrumentation.recordSpanError(span, err);
        throw err;
      } finally {
        span.end();
      }
    };
  }

  private createRequestWrapper(
    nc: Nats.NatsConnection,
    original: Nats.NatsConnection["request"]
  ) {
    const instrumentation = this;
    return async function request(
      this: Nats.NatsConnection,
      subject: string,
      data?: Uint8Array,
      opts?: Nats.RequestOptions
    ): Promise<Nats.Msg> {
      const span = instrumentation.tracer.startSpan(`${subject} request`, {
        attributes: {
          ...utils.baseTraceAttrs(nc.info),
        },
        kind: SpanKind.CLIENT,
      });
      const spanContext = trace.setSpan(context.active(), span);
      try {
        const res = await context.with(spanContext, () =>
          original.call(this, subject, data, opts)
        );
        span.setStatus({ code: SpanStatusCode.OK });
        return res;
      } catch (err) {
        instrumentation.recordSpanError(span, err);
        throw err;
      } finally {
        span.end();
      }
    };
  }

  private createSubscribeWrapper(
    nc: Nats.NatsConnection,
    original: Nats.NatsConnection["subscribe"]
  ) {
    const instrumentation = this;
    return function subscribe(
      this: Nats.NatsConnection,
      subject: string,
      opts?: Nats.SubscriptionOptions
    ): Nats.Subscription {
      const subscribeOpts = opts ? { ...opts } : undefined;
      if (subscribeOpts?.callback) {
        subscribeOpts.callback = instrumentation.wrapSubscriptionCallback(
          nc,
          subscribeOpts.callback
        );
      }
      const sub = original.call(this, subject, subscribeOpts);
      if (subscribeOpts?.callback) {
        return sub;
      }
      return instrumentation.wrapSubscription(nc, sub);
    };
  }

  private wrapSubscription(
    nc: Nats.NatsConnection,
    subscription: SubscriptionLike
  ): SubscriptionLike {
    if (!subscription || typeof subscription !== "object") {
      return subscription;
    }
    const instrumentation = this;
    return new Proxy(subscription as SubscriptionLike, {
      get(target, prop, receiver) {
        if (prop === Symbol.asyncIterator) {
          return function (...args: unknown[]) {
            const iterator = (target as any)[Symbol.asyncIterator](...args);
            return instrumentation.wrapAsyncIterator(nc, iterator);
          };
        }
        if (prop === "iterate") {
          return function (...args: unknown[]) {
            const iterator = (target as any).iterate(...args);
            return instrumentation.wrapAsyncIterator(nc, iterator);
          };
        }
        const value = Reflect.get(target, prop, receiver);
        if (typeof value === "function") {
          return value.bind(target);
        }
        return value;
      },
    });
  }

  private wrapAsyncIterator(
    nc: Nats.NatsConnection,
    iterator: AsyncMessageIterator
  ): AsyncMessageIterator {
    const instrumentation = this;
    const wrapped = (async function* (): AsyncMessageIterator {
      for await (let msg of iterator) {
        const { message, span } = instrumentation.prepareMessage(nc, msg);
        try {
          yield message;
          span.setStatus({ code: SpanStatusCode.OK });
        } catch (err) {
          instrumentation.recordSpanError(span, err);
          throw err;
        } finally {
          instrumentation.cleanupMessage(message);
          span.end();
        }
      }
    })();
    return wrapped;
  }

  private wrapJetStream(
    nc: Nats.NatsConnection,
    client: Nats.JetStreamClient
  ): Nats.JetStreamClient {
    if (!client || typeof client !== "object") {
      return client;
    }
    const instrumentation = this;
    return new Proxy(client, {
      get(target, prop, receiver) {
        switch (prop) {
          case "publish":
            return instrumentation.getJetStreamWrapper(target, prop, () =>
              instrumentation.createJetStreamPublishWrapper(
                nc,
                target.publish?.bind(target)
              )
            );
          case "subscribe":
            return instrumentation.getJetStreamWrapper(target, prop, () =>
              instrumentation.createJetStreamSubscribeWrapper(
                nc,
                target.subscribe?.bind(target)
              )
            );
          case "pullSubscribe":
            return instrumentation.getJetStreamWrapper(target, prop, () =>
              instrumentation.createJetStreamPullSubscribeWrapper(
                nc,
                target.pullSubscribe?.bind(target)
              )
            );
          case "fetch":
            return instrumentation.getJetStreamWrapper(target, prop, () =>
              instrumentation.createJetStreamFetchWrapper(
                nc,
                target.fetch?.bind(target)
              )
            );
          case "pull":
            return instrumentation.getJetStreamWrapper(target, prop, () =>
              instrumentation.createJetStreamPullWrapper(
                nc,
                target.pull?.bind(target)
              )
            );
          default:
            const value = Reflect.get(target, prop, receiver);
            if (typeof value === "function") {
              return value.bind(target);
            }
            return value;
        }
      },
    });
  }

  private createJetStreamPublishWrapper(
    nc: Nats.NatsConnection,
    original?: Nats.JetStreamClient["publish"]
  ) {
    if (!original) {
      return undefined;
    }
    const instrumentation = this;
    return async function publish(
      this: Nats.JetStreamClient,
      subject: string,
      data?: Uint8Array,
      options?: Partial<Nats.JetStreamPublishOptions>
    ): Promise<Nats.PubAck> {
      const publishOptions = options ? { ...options } : undefined;
      const { span, context: spanContext, headers } =
        instrumentation.startProducerSpan(nc, subject, data, undefined, publishOptions);
      try {
        const result = await context.with(spanContext, () =>
          original.call(this, subject, data, headers
            ? { ...(publishOptions ?? {}), headers }
            : publishOptions)
        );
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        instrumentation.recordSpanError(span, err);
        throw err;
      } finally {
        span.end();
      }
    };
  }

  private createJetStreamSubscribeWrapper(
    nc: Nats.NatsConnection,
    original?: Nats.JetStreamClient["subscribe"]
  ) {
    if (!original) {
      return undefined;
    }
    const instrumentation = this;
    return async function subscribe(
      this: Nats.JetStreamClient,
      subject: string,
      opts?: Nats.ConsumerOptsBuilder | Partial<Nats.ConsumerOpts>
    ): Promise<Nats.JetStreamSubscription> {
      instrumentation.wrapJetStreamCallbackOptions(nc, opts);
      const subscription = await original.call(this, subject, opts);
      return instrumentation.wrapSubscription(nc, subscription) as Nats.JetStreamSubscription;
    };
  }

  private createJetStreamPullSubscribeWrapper(
    nc: Nats.NatsConnection,
    original?: Nats.JetStreamClient["pullSubscribe"]
  ) {
    if (!original) {
      return undefined;
    }
    const instrumentation = this;
    return async function pullSubscribe(
      this: Nats.JetStreamClient,
      subject: string,
      opts: Nats.ConsumerOptsBuilder | Partial<Nats.ConsumerOpts>
    ): Promise<Nats.JetStreamPullSubscription> {
      instrumentation.wrapJetStreamCallbackOptions(nc, opts);
      const subscription = await original.call(this, subject, opts);
      return instrumentation.wrapSubscription(nc, subscription) as Nats.JetStreamPullSubscription;
    };
  }

  private createJetStreamFetchWrapper(
    nc: Nats.NatsConnection,
    original?: Nats.JetStreamClient["fetch"]
  ) {
    if (!original) {
      return undefined;
    }
    const instrumentation = this;
    return function fetch(
      this: Nats.JetStreamClient,
      stream: string,
      durable: string,
      opts?: Partial<Nats.PullOptions>
    ): AsyncMessageIterator {
      const iterator = original.call(this, stream, durable, opts) as AsyncMessageIterator;
      return instrumentation.wrapAsyncIterator(nc, iterator);
    };
  }

  private createJetStreamPullWrapper(
    nc: Nats.NatsConnection,
    original?: Nats.JetStreamClient["pull"]
  ) {
    if (!original) {
      return undefined;
    }
    const instrumentation = this;
    return async function pull(
      this: Nats.JetStreamClient,
      stream: string,
      durable: string
    ): Promise<Nats.JsMsg> {
      const { span, context: spanContext } = instrumentation.startProcessSpanForPull(
        nc,
        stream
      );
      try {
        const msg = await context.with(spanContext, () =>
          original.call(this, stream, durable)
        );
        span.setStatus({ code: SpanStatusCode.OK });
        const { message } = instrumentation.prepareMessage(nc, msg);
        // End the span immediately since pull retrieves a single message.
        instrumentation.cleanupMessage(message);
        span.end();
        return message as Nats.JsMsg;
      } catch (err) {
        instrumentation.recordSpanError(span, err);
        span.end();
        throw err;
      }
    };
  }

  private wrapJetStreamCallbackOptions(
    nc: Nats.NatsConnection,
    opts?: Nats.ConsumerOptsBuilder | Partial<Nats.ConsumerOpts>
  ) {
    if (!opts || typeof opts !== "object") {
      return;
    }
    const instrumentation = this;
    const maybeWrap = (container: any) => {
      if (typeof container.callbackFn === "function") {
        container.callbackFn = instrumentation.wrapSubscriptionCallback(
          nc,
          container.callbackFn
        );
      }
    };
    if (typeof (opts as Nats.ConsumerOptsBuilder).getOpts === "function") {
      const builder = opts as Nats.ConsumerOptsBuilder & {
        callback?: (fn: Nats.JsMsgCallback) => void;
        callbackFn?: Nats.JsMsgCallback;
        __otel_wrapped_callback__?: boolean;
      };
      maybeWrap(builder);
      if (!builder.__otel_wrapped_callback__ && typeof builder.callback === "function") {
        const originalCallbackSetter = builder.callback;
        builder.callback = function (fn: Nats.JsMsgCallback) {
          return originalCallbackSetter.call(
            this,
            instrumentation.wrapSubscriptionCallback(nc, fn)
          );
        };
        Object.defineProperty(builder, "__otel_wrapped_callback__", {
          value: true,
          configurable: false,
          enumerable: false,
          writable: false,
        });
      }
      return;
    }
    maybeWrap(opts);
  }

  private wrapSubscriptionCallback<T extends Message>(
    nc: Nats.NatsConnection,
    callback: (err: Nats.NatsError | null, msg: T) => void
  ) {
    const instrumentation = this;
    return function wrappedCallback(
      this: unknown,
      err: Nats.NatsError | null,
      msg: T
    ) {
      if (err) {
        callback.call(this, err, msg);
        return;
      }
      const { message, span, context: messageContext } =
        instrumentation.prepareMessage(nc, msg);
      try {
        context.with(messageContext, () => callback.call(this, null, message));
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (error) {
        instrumentation.recordSpanError(span, error);
        throw error;
      } finally {
        instrumentation.cleanupMessage(message);
        span.end();
      }
    };
  }

  private prepareMessage(
    nc: Nats.NatsConnection,
    msg: Message
  ): { message: Message; span: Span; context: Context } {
    const instrumentedMsg = this.setupMessage(msg, nc);
    const { span, context: messageContext } = this.startProcessSpan(
      nc,
      instrumentedMsg
    );
    this._messageContexts.set(instrumentedMsg as object, messageContext);
    return { message: instrumentedMsg, span, context: messageContext };
  }

  private startProcessSpan(
    nc: Nats.NatsConnection,
    msg: Message
  ): { span: Span; context: Context } {
    const carrier = msg.headers;
    const parentContext = carrier
      ? propagation.extract(ROOT_CONTEXT, carrier, utils.natsContextGetter)
      : ROOT_CONTEXT;
    const attributes = {
      ...utils.traceAttrs(nc.info, msg),
      [SemanticAttributes.MESSAGING_OPERATION]: "process",
      [SemanticAttributes.MESSAGING_DESTINATION_KIND]: "topic",
    };
    const kind = msg.reply ? SpanKind.SERVER : SpanKind.CONSUMER;
    const span = this.tracer.startSpan(`${msg.subject} process`, {
      attributes,
      kind,
    }, parentContext);
    const ctx = trace.setSpan(parentContext, span);
    return { span, context: ctx };
  }

  private startProcessSpanForPull(
    nc: Nats.NatsConnection,
    stream: string
  ): { span: Span; context: Context } {
    const span = this.tracer.startSpan(`${stream} pull`, {
      attributes: {
        ...utils.baseTraceAttrs(nc.info),
        [SemanticAttributes.MESSAGING_OPERATION]: "process",
        [SemanticAttributes.MESSAGING_DESTINATION_KIND]: "topic",
        [SemanticAttributes.MESSAGING_DESTINATION]: stream,
      },
      kind: SpanKind.CONSUMER,
    });
    const ctx = trace.setSpan(context.active(), span);
    return { span, context: ctx };
  }

  private startProducerSpan(
    nc: Nats.NatsConnection,
    subject: string,
    data?: Uint8Array,
    reply?: string,
    options?: { headers?: Nats.MsgHdrs }
  ): { span: Span; context: Context; headers?: Nats.MsgHdrs } {
    const isTemporaryDestination = this.isTemporaryDestination(subject);
    const destination = isTemporaryDestination ? "(temporary)" : subject;
    const span = this.tracer.startSpan(`${destination} send`, {
      attributes: {
        ...utils.baseTraceAttrs(nc.info),
        [SemanticAttributes.MESSAGING_DESTINATION_KIND]: "topic",
        [SemanticAttributes.MESSAGING_DESTINATION]: destination,
        [SemanticAttributes.MESSAGING_TEMP_DESTINATION]: isTemporaryDestination,
        [SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]: data
          ? data.length
          : 0,
      },
      kind: SpanKind.PRODUCER,
    });
    if (isTemporaryDestination) {
      span.setAttribute(SemanticAttributes.MESSAGING_CONVERSATION_ID, subject);
    } else if (reply) {
      span.setAttribute(SemanticAttributes.MESSAGING_CONVERSATION_ID, reply);
    }
    const spanContext = trace.setSpan(context.active(), span);
    const headers = options?.headers
      ? options.headers
      : this._natsHelpers.headers?.();
    if (headers) {
      propagation.inject(spanContext, headers, utils.natsContextSetter);
    }
    return { span, context: spanContext, headers };
  }

  private recordSpanError(span: Span, err: unknown) {
    const error = err instanceof Error ? err : new Error(String(err));
    span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
    span.recordException(error);
  }

  private setupMessage(msg: Message, nc: Nats.NatsConnection): Message {
    if (msg.reply) {
      this._natsHelpers.replySubjects.add(msg.reply);
    }
    const instrumentation = this;
    const handler: ProxyHandler<any> = {
      get(target, prop, receiver) {
        if (prop === "respond") {
          const originalRespond = Reflect.get(target, prop, receiver);
          if (typeof originalRespond !== "function") {
            return originalRespond;
          }
          return instrumentation.wrapRespond(originalRespond, nc, receiver as Message);
        }
        if (prop === "msg") {
          const raw = Reflect.get(target, prop, receiver);
          if (raw && typeof raw === "object") {
            const wrappedRaw = instrumentation.setupMessage(raw, nc);
            const parentCtx = instrumentation._messageContexts.get(
              receiver as object
            );
            if (parentCtx) {
              instrumentation._messageContexts.set(wrappedRaw as object, parentCtx);
            }
            return wrappedRaw;
          }
          return raw;
        }
        const value = Reflect.get(target, prop, receiver);
        if (typeof value === "function") {
          return value.bind(target);
        }
        return value;
      },
    };

    return new Proxy(msg as any, handler);
  }

  private cleanupMessage(msg: Message) {
    if (msg.reply) {
      this._natsHelpers.replySubjects.delete(msg.reply);
    }
    this._messageContexts.delete(msg as object);
  }

  private wrapRespond(
    originalFunc: Nats.Msg["respond"],
    nc: Nats.NatsConnection,
    proxyMsg: Message
  ) {
    const instrumentation = this;
    return function respond(
      this: Nats.Msg,
      data?: Uint8Array,
      options?: Nats.PublishOptions
    ): boolean {
      const msg = proxyMsg;
      const replySubject = msg.reply || "(temporary)";
      const span = instrumentation.tracer.startSpan(`${replySubject} send`, {
        attributes: {
          ...utils.baseTraceAttrs(nc.info),
          [SemanticAttributes.MESSAGING_DESTINATION_KIND]: "topic",
          [SemanticAttributes.MESSAGING_DESTINATION]: replySubject,
          [SemanticAttributes.MESSAGING_TEMP_DESTINATION]: true,
          [SemanticAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES]: data
            ? data.length
            : 0,
          [SemanticAttributes.MESSAGING_CONVERSATION_ID]: msg.reply || replySubject,
        },
        kind: SpanKind.PRODUCER,
      });
      const parentContext =
        instrumentation._messageContexts.get(msg as object) ?? context.active();
      const spanContext = trace.setSpan(parentContext, span);
      const headers = options?.headers
        ? options.headers
        : msg.headers ?? instrumentation._natsHelpers.headers?.();
      const finalOptions = headers
        ? { ...(options ?? {}), headers }
        : options;
      if (headers) {
        propagation.inject(spanContext, headers, utils.natsContextSetter);
      }
      try {
        return context.with(spanContext, () =>
          originalFunc.call(this, data, finalOptions)
        );
      } catch (err) {
        instrumentation.recordSpanError(span, err);
        throw err;
      } finally {
        span.end();
      }
    };
  }

  private isTemporaryDestination(subject: string) {
    return this._natsHelpers.replySubjects.has(subject);
  }

  ensureWrapped(
    moduleVersion: string | undefined,
    obj: any,
    methodName: string,
    wrapper: (original: any) => any
  ) {
    diag.debug(`Applying ${methodName} patch for nats@${moduleVersion}`);
    if (isWrapped(obj[methodName])) {
      this._unwrap(obj, methodName);
    }
    this._wrap(obj, methodName, wrapper);
  }
}
