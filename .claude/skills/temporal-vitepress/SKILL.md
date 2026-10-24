---
name: temporal-vitepress
description: How the temporal-easy VitePress site is built, and how to add, build and check a Temporal feature guide on it. Use after writing or changing pages in docs/, or when changing the site's config, theme or layout.
---

# Persona

You are a senior developer maintaining a small, plain documentation website. Keep it simple: stock VitePress theme, no extra plugins, nothing fancy.

# Goal

Every Temporal feature guide in `docs/<feature>/` is published on the site without editing the config, and the site always builds.

# How the site works

```
temporal-easy/
├── package.json             # docs:dev / docs:build / docs:preview → vitepress <cmd> docs
├── .npmrc                   # public npm registry (the global Artifactory one is unreachable)
├── pnpm-workspace.yaml      # allowBuilds: esbuild
├── topics/<feature>.md      # sources for the temporal-doc command
└── docs/                    # VitePress source root
    ├── .vitepress/config.mts
    ├── .vitepress/theme/    # index.ts, custom.css, GuideCards.vue
    ├── index.md             # home page: <GuideCards />
    └── <feature>/{docs,img}
```

- `config.mts` scans `docs/*/docs/README.md`. Each folder found is a guide:
  - a sidebar group titled from the folder name (`task-queues` → "Task Queues"): Overview (README) + every `NN-*.md` chapter, labelled with its `# ` heading up to the first `:`.
  - a card on the home page (title + chapter count), rendered by `GuideCards.vue` from `themeConfig.guides`.
- `guideOrder` in `config.mts` sets the order of known guides; others follow alphabetically.
- The sidebar starts with "Home" (`/`) and shows on every page. The header has no nav links: only search and the light/dark toggle.
- `custom.css`: JetBrains Mono (self-hosted via `@fontsource/jetbrains-mono`) for code, and a 960px content column so diagrams stay readable.
- `ignoreDeadLinks: 'localhostLinks'`: links to the local Temporal UI are fine; every other dead link fails the build.

# Instructions

Use pnpm, never npm.

To publish a new feature guide:
- Check it follows the `temporal-guide-writing` structure: `docs/<feature>/docs/README.md`, `NN-slug.md` chapters, `docs/<feature>/img/*.svg`.
- Add `<feature>` to `guideOrder` only if it must not be sorted alphabetically.
- Add the companion-guide line to the other guides' README blockquotes if it helps readers.

Then verify:
- `pnpm install` (once), then `pnpm docs:build`. It must end with "build complete". Dead links and Vue template errors fail here: fix the Markdown (relative `.md` links, backticks around `{{ }}` and `<Tags>`), never silence the check.
- `pnpm docs:dev` and look at the pages: the new sidebar group, the new home card, and every diagram. If you take headless Chrome screenshots, Chrome writes the PNG but does not exit: start it in the background, wait for the file, then kill it.

When changing the site itself:
- Keep the stock theme. Use VitePress CSS variables (`--vp-c-*`) so light and dark mode both work.
- Keep everything generated from the folders. Never hard-code a guide in the config, the sidebar or the home page.
- Update the root `README.md` (layout tree and Contributing) when the structure changes.
