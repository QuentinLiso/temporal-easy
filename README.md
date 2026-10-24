# temporal-easy

Onboarding guides for Temporal with Go:

- `docs/workflows/`: Temporal Workflows (13 chapters)
- `docs/activities/`: Temporal Activities (10 chapters)

The guides are plain Markdown. [VitePress](https://vitepress.dev) turns them into a static website with a left sidebar, search and a per-page table of contents.

## How the site is built

```
temporal-easy/
├── package.json             # scripts + vitepress dependency
├── .npmrc                   # project registry (public npm, not Artifactory)
├── pnpm-workspace.yaml      # pnpm settings (allows esbuild's install script)
└── docs/                    # VitePress source root
    ├── .vitepress/config.mts  # site title, top nav, sidebar
    ├── index.md               # landing page
    ├── workflows/
    │   ├── docs/*.md          # chapters (README.md is the overview)
    │   └── img/*.svg          # diagrams
    └── activities/
        ├── docs/*.md
        └── img/*.svg
```

VitePress builds every `.md` file into a page at the same path, so `docs/workflows/docs/05-timers-selectors-concurrency.md` is served at `/workflows/docs/05-timers-selectors-concurrency`. This `README.md` lives outside `docs/`, so it is not part of the site.

```sh
pnpm install        # once
pnpm docs:dev       # live-reload server on http://localhost:5173
pnpm docs:build     # static site in docs/.vitepress/dist/
pnpm docs:preview   # serve the built site on http://localhost:4173
```

The project uses pnpm. The local `.npmrc` points it at the public npm registry, which overrides the global Artifactory registry for this folder only.

`docs/.vitepress/dist/` is plain HTML, CSS and JS, so you can deploy it to any static host (GitLab Pages, GitHub Pages, S3 or nginx).

## Contributing

- **New chapter:**
  - Add `NN-short-slug.md` to `docs/<guide>/docs/` and start it with a `# NN · Title` heading.
  - Add the page to that guide's sidebar in `docs/.vitepress/config.mts`. Pages are not picked up automatically.
- **Diagrams:** put SVGs in the guide's `img/` folder and reference them with `![alt](../img/name.svg)`.
- **Links:** use relative links to the `.md` file, such as `[Versioning](./11-versioning.md)` or `[Activities](../../activities/docs/README.md)`.
- **Code samples:**
  - Use fenced blocks with a language (`` ```go ``).
  - Use production-like names such as `ProcessInvoiceWorkflow`, not `YourWorkflow`.
- **Vue syntax:** VitePress compiles Markdown through Vue. Put `{{ }}` and anything that looks like an HTML tag (`<T>`, `<nil>`) in prose inside backticks.
- **Before pushing:** run `pnpm docs:build`. It fails on dead internal links. Links to the local Temporal UI (`http://localhost:8233`) are allowed.
