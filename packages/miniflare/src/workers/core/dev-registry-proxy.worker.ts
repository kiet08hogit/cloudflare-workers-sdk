import { RpcTarget, WorkerEntrypoint } from "cloudflare:workers";
import { getQueueServiceName, HEADER_QUEUE_NAME } from "../queues/constants";
import { CoreBindings, CoreHeaders, CorePaths } from "./constants";
import {
	findQueueConsumer,
	openDebugPortClient,
	resolveSharedStorageOwner,
	resolveTarget,
	setRegistry,
	tailEventsReplacer,
	tailEventsReviver,
	workerNotFoundMessage,
} from "./dev-registry-proxy-shared.worker";
import type {
	RegistryEntry,
	WorkerdDebugPortConnector,
} from "./dev-registry-proxy-shared.worker";

export {
	createProxyDurableObjectClass,
	setRegistry,
} from "./dev-registry-proxy-shared.worker";

const HANDLER_RESERVED_KEYS = new Set([
	"alarm",
	"connect",
	"self",
	"tail",
	"tailStream",
	"test",
	"trace",
	"webSocketClose",
	"webSocketError",
	"webSocketMessage",
]);

interface Env {
	[CoreBindings.DATA_DEV_REGISTRY_SECRET]: ArrayBuffer;
	DEV_REGISTRY_DEBUG_PORT: WorkerdDebugPortConnector;
	DEV_REGISTRY_INSTANCE_ID: string;
	MINIFLARE_LOOPBACK: Fetcher;
}

interface Props {
	service: string;
	entrypoint: string | null;
	// User-supplied `props` from the original service binding / tail consumer.
	// Forwarded to the remote entrypoint via the debug port so they are
	// available as `ctx.props` on the callee.
	userProps?: Record<string, unknown>;
	// Is this trying to access a "storage" miniflare service?
	// If it is, the proxy will try to forward to the shared storage owner
	// (first active worker in the dev registry)
	storage?: boolean;
	storageScope?: string;
	rpcProperties?: string[];
}

class ExternalRpcTarget extends RpcTarget {
	constructor(resolve: () => object) {
		super();
		return new Proxy(this, {
			get(target, prop) {
				if (Reflect.has(target, prop)) {
					return Reflect.get(target, prop);
				}
				return Reflect.get(resolve(), prop);
			},
		});
	}
}

function getTarget(props: Props): RegistryEntry | undefined {
	if (props.storage) {
		return props.storageScope === undefined
			? undefined
			: resolveSharedStorageOwner(props.storageScope);
	}
	return resolveTarget(props.service);
}

/**
 * workerd pins a cross-process RPC session to every object result, including
 * plain data, until `Symbol.dispose` runs. Stubs and functions must keep that
 * session. Dispose is not itself a stub: plain results carry it too.
 */
function isLiveRpcHandle(value: object): boolean {
	if (value instanceof RpcTarget) {
		return true;
	}
	const name = value.constructor?.name;
	return name === "RpcStub" || name === "RpcProperty" || name === "RpcPromise";
}

function isPlainData(value: object): boolean {
	if (Array.isArray(value) || ArrayBuffer.isView(value)) {
		return true;
	}
	if (
		value instanceof Date ||
		value instanceof Map ||
		value instanceof Set ||
		value instanceof ArrayBuffer
	) {
		return true;
	}
	const proto = Object.getPrototypeOf(value) as object | null;
	return proto === Object.prototype || proto === null;
}

function containsLiveRpcHandle(
	value: unknown,
	seen = new Set<object>()
): boolean {
	if (typeof value === "function") {
		return true;
	}
	if (value === null || typeof value !== "object") {
		return false;
	}
	if (seen.has(value)) {
		return false;
	}
	seen.add(value);
	if (!isPlainData(value) || isLiveRpcHandle(value)) {
		return true;
	}
	if (value instanceof Map) {
		for (const [key, child] of value) {
			if (
				containsLiveRpcHandle(key, seen) ||
				containsLiveRpcHandle(child, seen)
			) {
				return true;
			}
		}
		return false;
	}
	if (value instanceof Set) {
		for (const child of value) {
			if (containsLiveRpcHandle(child, seen)) {
				return true;
			}
		}
		return false;
	}
	if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
		return false;
	}
	for (const key of Reflect.ownKeys(value)) {
		if (key === Symbol.dispose || key === Symbol.asyncDispose) {
			continue;
		}
		const child = (value as Record<string | symbol, unknown>)[key];
		if (typeof child === "function" || containsLiveRpcHandle(child, seen)) {
			return true;
		}
	}
	return false;
}

/**
 * Copy a plain RPC result and release its session. Live stubs are returned
 * as-is so their connection stays up. The cached debug-port client is not
 * touched.
 *
 * @param result - Value returned by a remote service-binding call.
 * @returns A clone when `result` is plain data, otherwise `result`.
 */
function releasePlainRpcResult<T>(result: T): T {
	if (result === null || typeof result !== "object") {
		return result;
	}
	if (containsLiveRpcHandle(result)) {
		return result;
	}
	const dispose = (result as { [Symbol.dispose]?: () => void })[Symbol.dispose];
	try {
		const copy = structuredClone(snapshotPlainData(result));
		dispose?.call(result);
		return copy as T;
	} catch {
		return result;
	}
}

function snapshotPlainData(
	value: unknown,
	seen = new Map<object, unknown>()
): unknown {
	if (value === null || typeof value !== "object") {
		return value;
	}
	const existing = seen.get(value);
	if (existing !== undefined) {
		return existing;
	}
	if (value instanceof Date) {
		return new Date(value.getTime());
	}
	if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) {
		return structuredClone(value);
	}
	if (value instanceof Map) {
		const copy = new Map();
		seen.set(value, copy);
		for (const [key, child] of value) {
			copy.set(snapshotPlainData(key, seen), snapshotPlainData(child, seen));
		}
		return copy;
	}
	if (value instanceof Set) {
		const copy = new Set();
		seen.set(value, copy);
		for (const child of value) {
			copy.add(snapshotPlainData(child, seen));
		}
		return copy;
	}
	if (Array.isArray(value)) {
		const copy: unknown[] = [];
		seen.set(value, copy);
		for (const child of value) {
			copy.push(snapshotPlainData(child, seen));
		}
		return copy;
	}
	const copy: Record<string, unknown> = {};
	seen.set(value, copy);
	for (const key of Object.keys(value)) {
		copy[key] = snapshotPlainData(
			(value as Record<string, unknown>)[key],
			seen
		);
	}
	return copy;
}

/**
 * Forward one remote property. Methods run with the remote fetcher as `this`,
 * then plain results are released.
 */
function forwardRemoteProperty(
	fetcher: Fetcher,
	prop: string | symbol
): unknown {
	const value: unknown = Reflect.get(fetcher, prop);
	if (typeof value !== "function") {
		return releasePlainRpcResult(value);
	}
	return (...args: unknown[]) => {
		const result: unknown = Reflect.apply(value, fetcher, args);
		if (
			typeof result === "object" &&
			result !== null &&
			"then" in result &&
			typeof result.then === "function"
		) {
			return (result as Promise<unknown>).then((resolved) =>
				releasePlainRpcResult(resolved)
			);
		}
		return releasePlainRpcResult(result);
	};
}

function resolve(props: Props, env: Env, target: RegistryEntry): Fetcher {
	const { service, entrypoint, userProps, storage } = props;

	const serviceName = storage
		? service
		: entrypoint === null || entrypoint === "default"
			? target.defaultEntrypointService
			: target.userWorkerService;
	const client = openDebugPortClient(
		env.DEV_REGISTRY_DEBUG_PORT,
		target,
		env.DEV_REGISTRY_INSTANCE_ID
	);
	return client.getEntrypoint(serviceName, entrypoint ?? undefined, userProps);
}

/**
 * Relays a queue broker's `/message` or `/batch` request to the dev session
 * consuming that queue. The queue name comes from a request header (rather
 * than binding props) because the broker serves every queue in its process
 * through a single binding. Responds with 503 when no running dev session
 * advertises a consumer for the queue, in which case the sending broker drops
 * the message, mirroring the local no-consumer behaviour.
 */
export class ExternalQueueProxy extends WorkerEntrypoint<Env> {
	fetch(request: Request): Promise<Response> | Response {
		const queueName = request.headers.get(HEADER_QUEUE_NAME);
		if (queueName === null) {
			return new Response(`Missing "${HEADER_QUEUE_NAME}" header`, {
				status: 400,
			});
		}

		const target = findQueueConsumer(queueName);
		if (target === undefined) {
			return new Response(
				`No Worker consuming queue "${queueName}" found in the local dev registry. Make sure the consumer Worker is running locally.`,
				{ status: 503 }
			);
		}

		const client = openDebugPortClient(
			this.env.DEV_REGISTRY_DEBUG_PORT,
			target,
			this.env.DEV_REGISTRY_INSTANCE_ID
		);
		const broker = client.getEntrypoint(getQueueServiceName(queueName));
		const headers = new Headers(request.headers);
		headers.delete(HEADER_QUEUE_NAME);
		return broker.fetch(new Request(request, { headers }));
	}
}

export class ExternalServiceProxy extends WorkerEntrypoint<Env, Props> {
	_fetcher: Fetcher | undefined;
	_targetAddress: string | undefined;
	_targetInstanceId: string | undefined;
	_entryFetcher: Fetcher | null = null;

	constructor(ctx: ExecutionContext<Props>, env: Env) {
		super(ctx, env);

		// Separate connection for scheduled: the debug port's EventDispatcher
		// doesn't support runScheduled/runAlarm/queue, so we forward via HTTP.
		const target = resolveTarget(ctx.props.service);
		if (target && target.debugPortAddress) {
			const client = openDebugPortClient(
				env.DEV_REGISTRY_DEBUG_PORT,
				target,
				env.DEV_REGISTRY_INSTANCE_ID
			);
			this._entryFetcher = client.getEntrypoint("core:entry");
		}

		return new Proxy(this, {
			get(target, prop) {
				if (Reflect.has(target, prop)) {
					return Reflect.get(target, prop);
				}
				if (typeof prop === "string" && HANDLER_RESERVED_KEYS.has(prop)) {
					return undefined;
				}
				// A debug-port RpcProperty cannot itself cross this outer property path.
				// Terminate the outer hop with a local target, then forward its methods.
				if (
					typeof prop === "string" &&
					ctx.props.rpcProperties?.includes(prop)
				) {
					return new ExternalRpcTarget(() => {
						const fetcher = target._resolve();
						if (!fetcher) {
							throw new Error(workerNotFoundMessage(ctx.props.service));
						}
						return Reflect.get(fetcher, prop) as object;
					});
				}

				const fetcher = target._resolve();
				if (!fetcher) {
					throw new Error(workerNotFoundMessage(ctx.props.service));
				}
				return forwardRemoteProperty(fetcher, prop);
			},
		});
	}

	_resolve(): Fetcher | null {
		const target = getTarget(this.ctx.props);
		if (target === undefined || !target.debugPortAddress) {
			this._fetcher = undefined;
			this._targetAddress = undefined;
			this._targetInstanceId = undefined;
			return null;
		}
		if (
			this._fetcher !== undefined &&
			this._targetAddress === target.debugPortAddress &&
			this._targetInstanceId === target.instanceId
		) {
			return this._fetcher;
		}

		this._fetcher = resolve(this.ctx.props, this.env, target);
		this._targetAddress = target.debugPortAddress;
		this._targetInstanceId = target.instanceId;
		return this._fetcher;
	}

	fetch(request: Request): Promise<Response> | Response {
		const fetcher = this._resolve();
		if (!fetcher) {
			return new Response(workerNotFoundMessage(this.ctx.props.service), {
				status: 503,
			});
		}
		return fetcher.fetch(request);
	}

	async scheduled(controller: ScheduledController) {
		if (!this._entryFetcher) {
			throw new Error(workerNotFoundMessage(this.ctx.props.service));
		}
		const params = new URLSearchParams();
		if (controller.cron) {
			params.set("cron", controller.cron);
		}
		if (controller.scheduledTime) {
			params.set("time", String(controller.scheduledTime));
		}
		const response = await this._entryFetcher.fetch(
			new Request(`http://localhost${CorePaths.SCHEDULED}?${params}`, {
				headers: { "MF-Route-Override": this.ctx.props.service },
			})
		);
		if (!response.ok) {
			const body = await response.text();
			throw new Error(
				`Scheduled handler returned HTTP ${response.status}: ${body}`
			);
		}
	}

	// Forward tail events to the remote worker via RPC.
	// Events with rpcMethod==="tail" are filtered out to prevent infinite
	// recursion (the remote tail() call would itself produce a tail event).
	async tail(events: TraceItem[]) {
		const fetcher = this._resolve();
		if (fetcher === null) {
			return;
		}
		const filtered = events.filter(
			(e) => (e.event as { rpcMethod?: string } | null)?.rpcMethod !== "tail"
		);
		if (filtered.length === 0) {
			return;
		}
		try {
			const serializedEvents = JSON.parse(
				JSON.stringify(filtered, tailEventsReplacer),
				tailEventsReviver
			);
			// `await` rather than `return`: the RPC rejects when the peer's debug
			// port has gone away, and returning the promise leaves that rejection
			// outside this `try`, so it escapes as an unhandled rejection instead of
			// being reported.
			// @ts-expect-error .tail is not in the `Fetcher` type but it's a valid RPC call
			await fetcher.tail(serializedEvents);
		} catch (e) {
			console.warn(
				`[dev-registry] Failed to forward tail events to "${
					this.ctx.props.service
				}": ${e instanceof Error ? e.message : String(e)}`
			);
		}
	}
}

export default {
	async fetch(request, env) {
		const supplied = new TextEncoder().encode(
			request.headers.get(CoreHeaders.DEV_REGISTRY_SECRET) ?? ""
		);
		const expected = env[CoreBindings.DATA_DEV_REGISTRY_SECRET];
		if (
			expected === undefined ||
			supplied.byteLength !== expected.byteLength ||
			!crypto.subtle.timingSafeEqual(supplied, expected)
		) {
			return new Response("Forbidden", { status: 403 });
		}
		const url = new URL(request.url);
		if (
			url.pathname !== "/" ||
			url.hostname !== "127.0.0.1" ||
			!/^127\.0\.0\.1(?::\d+)?$/.test(request.headers.get("Host") ?? "") ||
			request.headers.has("Origin")
		) {
			return new Response("Forbidden", { status: 403 });
		}
		if (request.method !== "POST") {
			return new Response("Not Found", { status: 404 });
		}
		const data = await request.json<Record<string, RegistryEntry>>();
		setRegistry(data);
		return new Response("ok");
	},
} satisfies ExportedHandler<Env>;
