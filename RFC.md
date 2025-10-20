# RFC: Update NATS.js instrumentation for modern client APIs

## Summary
This RFC proposes a revamp of the `@opentelemetry/instrumentation-nats` package so that it fully supports the latest NATS.js client surface area. The existing instrumentation was authored against early 2.x releases and only wraps a small subset of the connection methods. The new work will:

* preserve context and create telemetry for both callback based subscribers and the async iterator/`QueuedIterator` APIs introduced in NATS.js 2.x;
* cover the current request/reply helpers, including iterator based responders;
* add spans for JetStream `publish` and consumer APIs (`subscribe`, `pullSubscribe`, and iterator-based `fetch`); and
* provide complete integration test coverage against a real NATS server, including JetStream scenarios.

## Goals
* Support NATS.js 2.x+ connection primitives (`publish`, `subscribe`, `request`, `respond`) for both callback and async iterator consumers without breaking the Subscription surface (unsubscribe, drain, etc.).
* Instrument JetStream client publish/consume paths with the same propagation semantics as core NATS messaging.
* Ensure context propagation for responses sent through `Msg.respond` as well as iterator-based responders.
* Add comprehensive integration tests that execute against a running NATS Server with JetStream enabled, covering callback flows, async iterator flows, request/reply, JetStream push and pull consumers, and error paths.

## Non-goals
* Instrumenting NATS server management APIs (`jetstreamManager`, account info, etc.) beyond what is necessary for publish/consume spans.
* Supporting deprecated NATS.js < 2.x client APIs.
* Providing semantic conventions beyond the messaging attributes already emitted by the package.

## Background & Motivation
The current implementation wraps `subscribe`, `publish`, and `request` by returning ad-hoc async generators. This breaks a number of newer NATS.js capabilities (drain, unsubscribe, iterator helpers) and does not handle JetStream at all. Since NATS.js now encourages async iterator consumption and JetStream pull/push consumers, the instrumentation must hook deeper into the objects returned by the client instead of replacing them. Doing so also lets us share the same logic between callback and iterator styles and avoids regressions for future NATS releases.

## Detailed design

### Connection patching
* Continue patching `nats.connect`, but extend the proxy returned from `wrapConnect` to intercept:
  * `publish`, `subscribe`, `request` as today;
  * `jetstream` so the resulting `JetStreamClient` is wrapped; and
  * future proofing for `jetstreamManager` (no-op proxy so tests continue to use the wrapped connection).

### Publish spans
* Replace the current publish wrapper with a shared `instrumentPublish` helper that is used by both core NATS and JetStream `publish`.
* Continue tagging payload size, temporary reply subjects, and conversation IDs. Reuse logic for temporary destinations by keeping the `replySubjects` cache in helpers.
* Ensure propagation uses `MsgHdrs` from the client when available; fall back to lazily creating headers through the helper on both core NATS and JetStream paths.

### Subscribe spans (callback consumers)
* Keep wrapping user callbacks, but move the span creation into a reusable helper invoked for both standard `Subscription` and JetStream `JetStreamSubscription` callback registrations.
* Generate a CONSUMER or SERVER span depending on whether the inbound message has a reply subject.
* Use `context.with` to execute the user callback and end the span in a `finally` block, recording exceptions when thrown.

### Subscribe spans (async iterators & queued iterators)
* Instead of replacing the subscription with an async generator, create a proxy that:
  * delegates all properties and prototype methods to the original subscription; and
  * intercepts `Symbol.asyncIterator` (and `iterator()` / `iterate()` if present) to return an instrumented iterator.
* The instrumented iterator will:
  * await the underlying iterator result;
  * wrap each yielded `Msg`/`JsMsg` with `setupMessage` so `respond` continues to be instrumented;
  * start the processing span prior to yielding the message; and
  * end the span after the consumer resumes, handling exceptions and cleanup just like the callback path.
* Ensure that `next()`, `throw()`, and `return()` are delegated to the underlying iterator to preserve semantics for helpers like `sub.next()`.

### Request / respond
* Retain the existing CLIENT span for `NatsConnection.request`, but generalise so we can reuse it for `JetStreamClient.pull` (which sends a request to the server).
* Update `setupMessage` so it understands both `Msg` and `JsMsg`. For iterator-based responders we must bind `Msg.respond` to the active context so the propagation headers include the request span.
* Fix the skipped async iterator responder test by ensuring we bind the respond function to the per-message context when iterating.

### JetStream support
* Wrap `nc.jetstream()` to return a proxy that instruments:
  * `publish` (PRODUCER span, same helper as core publish);
  * `subscribe` and `pullSubscribe` (reuse subscription instrumentation helpers, accounting for promises that resolve to subscriptions);
  * `fetch` and `pull` which return async iterators/queued iterators. For each message emitted, start/end a CONSUMER span.
* Ensure ack/nak helpers on `JsMsg` continue to operate by not overriding those functions.

### Error handling and cleanup
* Maintain the `replySubjects` Set to detect temporary destinations. Extend cleanup to handle JetStream responders.
* Guard wrappers so double wrapping does not occur when instrumentation is reloaded.
* Surface instrumentation spans even when propagation headers are missing. Continue to record exceptions on failure paths.

## Testing strategy
* Use the existing contrib test utils to start a Dockerised NATS server with JetStream enabled (the default `nats` image exposes JetStream).
* Expand `test/nats.test.ts` to cover:
  1. Core publish/subscribe via callback.
  2. Core publish/subscribe via async iterator (`for await`).
  3. Request/reply via callback responder.
  4. Request/reply via async iterator responder (un-skip the existing test).
  5. JetStream publish + push subscribe via callback.
  6. JetStream publish + async iterator consumption via `for await`.
  7. JetStream pull consumer (`pullSubscribe` or `fetch`) returning an iterator.
  8. Error path when subscriber callback throws.
* Assert span relationships, attributes, and propagation for each case using the existing helper utilities.

## Implementation plan
1. Refactor instrumentation helpers in `src/instrumentation.ts`:
   * introduce shared helpers (`instrumentPublish`, `instrumentSubscription`, `instrumentIterator`);
   * update `wrapConnect` proxy to instrument jetstream accessors.
2. Extend `setupMessage` to understand `JsMsg` and keep context bindings for `respond`/ack functions.
3. Add JetStream-specific wrappers (`wrapJetStream`, `wrapJetStreamSubscription`, `wrapQueuedIterator`).
4. Update `utils.ts` if required to cover JetStream metadata (subject names, stream, consumer info attributes when available).
5. Write comprehensive integration tests in `test/nats.test.ts`, covering all flows described above.
6. Ensure lint/test scripts pass locally (`yarn lint`, `yarn test`).
7. Document the new behaviour in `README.md` if necessary (e.g., compatibility statement).
