---
name: temporal-guide-writing
description: Write a progressive onboarding roadmap for one Temporal feature in Go, from the Temporal Encyclopedia and Go SDK docs read side by side, with SVG diagrams and compilable Go examples. Use when documenting a Temporal feature into docs/<feature>/.
---

# Persona

You are a senior developer onboarding a junior developer into using Temporal with Go.

# Goal

Make a comprehensive and usable documentation about a Temporal feature that would serve as a roadmap.
The output goes in `docs/<feature>/docs/` (Markdown) and `docs/<feature>/img/` (SVG diagrams), where `<feature>` is the kebab-case feature name (e.g. `workflows`, `task-queues`).

# Instructions

Read every page of the Temporal Encyclopedia and of the Temporal Go SDK given for the feature.

They both work side by side: the Encyclopedia gives the concepts, the Go SDK gives how to do it in Go.
When they disagree, or the SDK has something the docs don't mention, check the SDK source and note it.

First, define the problem(s) it solves, or can solve.
Split the core concepts/features from implementation details and sub-features or options.
Use .svg diagrams when a section explains an architecture, a flow between components, or a mental model.
For each section that you will create, illustrate it with a working Go implementation based on the Temporal documentation.
Use illustrative examples:
- Do not use Go SDK abstract names like `YourWorkflowDefinition(...)`
- Instead, use production-like names like `PaymentWorkflowInput struct{...}` or `ProcessInvoiceWorkflow(...)`
- You can be a bit creative in the names as long as they sound real
Group this in a progressive way

# Output structure

```
docs/<feature>/
├── docs/
│   ├── README.md          # the roadmap / overview
│   ├── 01-<slug>.md       # chapters, in reading order
│   └── NN-<slug>.md
└── img/
    └── <feature>-<topic>.svg
```

## README.md (the overview)
- `# Temporal <Feature> with Go: a roadmap`
- "Why this matters, in 30 seconds".
- The roadmap: stages (Foundations → daily use → advanced → production), one table per stage: `# | Chapter | You'll be able to…`.
- Core concepts vs. implementation details table.
- A "Which … do I need?" decision tree in a `text` block.
- A Go API cheat sheet table: `I want to… | Go API | Chapter`.
- Notes about the code samples, "Notes from checking the docs against the SDK", and the sources.

## Chapters
- File `NN-short-slug.md`, first line `# NN · Title`. The sidebar label is the text before the first `:`, so keep that part short.
- A meta blockquote under the title: `**Level:** … · **Read time:** … · **Sources:** <links>`.
- Numbered `##` sections: start with "1. The problem", end with "Key takeaways".
- Link other chapters with relative `.md` links, other guides with `../../<feature>/docs/…`.

## Diagrams
- Hand-written SVG, `viewBox` about 960 wide, white background rect, readable font sizes (≥ 12px for body text).
- Put a `<title>` and `<desc>` in each SVG. Reference it as `![Alt text](../img/name.svg)`.
- Every SVG in `img/` must be used by at least one page.

## Go code
- Every Go block starts with `// file: <path>` so the reader can paste it into that path.
- All blocks of one chapter form one module, `github.com/acme/shop`, with realistic packages (`payments`, `shipping`, `cmd/worker`, …).
- External systems (payment gateways, mailers…) are interfaces or fakes.
- Compile and `go vet` each chapter: extract its blocks into a scratch module using the latest `go.temporal.io/sdk`, run `gofmt -l`, `go build ./...`, `go vet ./...`. Fix until clean. State the SDK version in the README.

## Writing for the website
The guides are rendered by VitePress (see the `temporal-vitepress` skill). In prose outside code, keep `{{ }}` and anything that looks like an HTML tag (`<T>`, `<nil>`) inside backticks.
