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
├── topics/<feature>.md      # sources of each guide, read by /temporal-doc
├── .claude/                 # /temporal-doc command + temporal-guide-writing and temporal-vitepress skills
└── docs/                    # VitePress source root
    ├── .vitepress/
    │   ├── config.mts         # site config; builds nav + sidebar from the folders
    │   └── theme/             # default theme + JetBrains Mono, wider content, GuideCards.vue
    ├── index.md               # home page: one card per guide, generated
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

- **New chapter:** add `NN-short-slug.md` to `docs/<guide>/docs/` and start it with a `# NN · Title` heading. The sidebar picks it up automatically and uses the heading text up to the first `:` as the label.
- **New Temporal feature:**
  - Create `docs/<feature>/docs/README.md` (the overview), the numbered chapters and `docs/<feature>/img/`.
  - The feature then appears automatically as its own sidebar section and as a card on the home page.
  - To control its position, add it to `guideOrder` in `docs/.vitepress/config.mts`. Unlisted features are added alphabetically.
- **Diagrams:** put SVGs in the guide's `img/` folder and reference them with `![alt](../img/name.svg)`.
- **Links:** use relative links to the `.md` file, such as `[Versioning](./11-versioning.md)` or `[Activities](../../activities/docs/README.md)`.
- **Code samples:**
  - Use fenced blocks with a language (`` ```go ``).
  - Use production-like names such as `ProcessInvoiceWorkflow`, not `YourWorkflow`.
- **Vue syntax:** VitePress compiles Markdown through Vue. Put `{{ }}` and anything that looks like an HTML tag (`<T>`, `<nil>`) in prose inside backticks.
- **Before pushing:** run `pnpm docs:build`. It fails on dead internal links. Links to the local Temporal UI (`http://localhost:8233`) are allowed.
- **Generating a guide with Claude Code:** write `topics/<feature>.md` (copy an existing one: feature name, Encyclopedia URLs, Go SDK URLs), then run `/temporal-doc topics/<feature>.md`.
