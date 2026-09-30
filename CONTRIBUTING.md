# Contributing

Use Python 3.12, Node 20 or newer, and the checked-in `uv.lock` / frontend `package-lock.json`.

```bash
make setup
cd apps/frontend && npx playwright install chromium webkit && cd ../..
make test
make production-config
```

On Linux, Playwright may also require `npx playwright install --with-deps chromium webkit`. Unit and browser tests use explicit fixtures; they require neither production weights nor a Telegram token. CI additionally builds the deployment containers. Keep the lightweight bot free of Torch/FAISS dependencies.

Changes should include an appropriate regression test and relevant API or setup documentation. Preserve the single-image API contract; show uncertainty explicitly. Do not log photos, Telegram tokens, chat contents, user identity or private filesystem paths. Never commit `.env`, downloaded images, indices, model weights, local environments or research caches. Secrets belong in deployment configuration; public frontend environment variables are visible to every visitor.

Frozen configs and model artifacts are evidence, not routine application configuration. Do not change their bytes or replace existing release assets while modifying UI, bot or API plumbing. A new model/gallery release needs versioned provenance, evaluation and independently checked hashes.

Historical `ml/research/` sources and generated data have separate environments and artifact-dependent procedures. They are excluded from default product lint/test discovery to preserve the source hashes used in recorded experiments. Run the exact commands in the associated research report when its local artifacts are available. Do not represent those reports as fresh independent evaluation, and do not rewrite negative results.

Before a public release, review the Git diff and file list, run the checks above, verify distribution URLs and runtime readiness, and perform a live Telegram smoke with the deployment's token. Do not commit or publish confidential sample photographs in bug reports. Code is MIT; model and dataset rights are separate.
