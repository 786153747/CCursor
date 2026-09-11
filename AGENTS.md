# CCursor Project Principles

These principles apply to the entire CCursor repository, including Cursor++ and installer.

## Ultimate Goal

CCursor is a lightweight extension and compatibility layer built on Cursor, not a replacement IDE or an independent agent platform.
Keep the plugin as light as possible while pursuing complete, reliable functionality and seamless integration with Cursor.
Rely on Cursor's native capabilities wherever they actually satisfy the requirement. For capabilities the plugin must implement itself, follow verifiable official Cursor protocols and behavior as closely as practical.
"Perfect" means correct, dependable, compatible, maintainable, and backed by evidence; it never means an unsupported claim of zero risk.

## Decision Order

1. Verify whether the installed Cursor version already provides the required capability through an accessible interface.
2. Reuse that capability instead of duplicating its implementation or data ownership.
3. If adaptation is needed, prefer a small, explicit adapter over a new subsystem.
4. Implement locally only the necessary gap, using verified official semantics as the reference.
5. When official behavior is unknown or unsuitable for this project, state the uncertainty and justify the smallest safe project-specific policy.

Prefer supported interfaces and existing protocol paths. Use invasive patches or private internals only when necessary, with narrow scope, version checks, and a clear failure mode.
Do not assume that a capability observed inside Cursor is exposed to extensions, works through the BYOK route, or can replace required server-side logic without verification.

## Keep the Plugin Light

- Prefer Cursor-owned tools, execution, UI, storage, and lifecycle management when a usable native interface meets the requirement.
- Do not create a parallel agent framework, duplicate source of truth, redundant persistence, hidden global state, speculative cache, or compatibility layer without a demonstrated need.
- Make ownership and lifetimes explicit. Retain only the working data needed for the operation; do not disguise shared state as run-local storage.
- Optimize for the least necessary mechanisms, dependencies, resource use, and maintenance burden, not merely the fewest lines of code.
- Do not remove necessary validation, cancellation, request correlation, or save confirmation merely to make the implementation look smaller.

## Correctness and Official Alignment

- Data integrity and user intent take priority over lightweight implementation or superficial similarity to official behavior.
- Missing required context must not silently become a shorter prompt. Failed saves must not be treated as successful checkpoint commits.
- Distinguish optional metadata from required execution data. Avoid stricter checks that reject otherwise valid Cursor workflows.
- Verify protocol encodings, identifiers, message ordering, cancellation, and acknowledgement semantics at their actual boundaries.
- Distinguish official client code, local-agent runtime code, observed behavior, and unknown cloud internals. Record the version behind each material assumption.
- Client KV support does not prove that official cloud servers lack persistence or caching. An ACK proves only what its protocol and implementation establish.
- Official alignment does not require copying every internal abstraction, cache, constant, or incidental defect.
- Never delete existing user data or recovery sources merely to simplify ownership. Destructive maintenance requires separate explicit approval.

## Implementation and Verification

- Before changing code, identify the concrete gap, the native capability considered, and why reuse, adaptation, or local implementation is necessary.
- Keep changes focused. Explain newly introduced mechanisms and distinguish fixed defects from engineering tradeoffs and unverified assumptions.
- Prefer focused behavioral and integration tests at real protocol boundaries. Do not use test volume or helper-only coverage as a substitute for correct wiring.
- Test failure, cancellation, concurrency, and cold restoration where they affect the change. Separate pre-existing failures from regressions.
- Preserve unrelated WIP, routes, provider credentials, and backups. Do not alter Cursor's live databases or restart Cursor without explicit authorization.
- Do not change versions, commit, push, or deploy unless the current task authorizes those actions.
