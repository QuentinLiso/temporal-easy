import { readdirSync, readFileSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig, type DefaultTheme } from 'vitepress'

const srcDir = fileURLToPath(new URL('..', import.meta.url))

// Guides listed first, in this order. Any other docs/<feature>/docs/ folder
// is picked up automatically and appended alphabetically.
const guideOrder = ['architecture', 'workflows', 'activities', 'client']

function guides(): string[] {
  const found = readdirSync(srcDir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(srcDir, d.name, 'docs', 'README.md')))
    .map((d) => d.name)
  const rank = (g: string) => (guideOrder.includes(g) ? guideOrder.indexOf(g) : guideOrder.length)
  return found.sort((a, b) => rank(a) - rank(b) || a.localeCompare(b))
}

// "workflows" -> "Workflows", "task-queues" -> "Task Queues"
function guideTitle(guide: string): string {
  return guide.split('-').map((w) => w[0].toUpperCase() + w.slice(1)).join(' ')
}

// "# 10 · Activity Operations: Pause, Unpause…" -> "10 · Activity Operations"
function chapterTitle(file: string): string {
  const h1 = readFileSync(file, 'utf8').match(/^# (.+)$/m)?.[1] ?? file
  return h1.split(':')[0].trim()
}

function chapterFiles(guide: string): string[] {
  return readdirSync(join(srcDir, guide, 'docs'))
    .filter((f) => /^\d+-.+\.md$/.test(f))
    .sort()
}

function guideSidebar(guide: string): DefaultTheme.SidebarItem {
  const dir = join(srcDir, guide, 'docs')
  const chapters = chapterFiles(guide).map((f) => ({
    text: chapterTitle(join(dir, f)),
    link: `/${guide}/docs/${f.replace(/\.md$/, '')}`,
  }))
  return {
    text: guideTitle(guide),
    collapsed: false,
    items: [{ text: 'Overview', link: `/${guide}/docs/README` }, ...chapters],
  }
}

// Card on the home page.
function guideCard(guide: string) {
  return {
    title: guideTitle(guide),
    link: `/${guide}/docs/README`,
    chapters: chapterFiles(guide).length,
  }
}

export default defineConfig({
  // GitHub Pages serves the site under /<repo>/. Must match the repository name.
  base: '/temporal-easy/',
  title: 'Temporal with Go',
  description: 'A progressive roadmap to Temporal Workflows and Activities in Go.',
  cleanUrls: true,
  srcExclude: ['**/node_modules/**'],
  // Temporal Web UI links (localhost:8233) are intentional.
  ignoreDeadLinks: 'localhostLinks',
  themeConfig: {
    search: { provider: 'local' },
    outline: [2, 3],
    // No nav links: the header's right side only shows the light/dark toggle.
    nav: [],
    sidebar: [{ text: 'Home', link: '/' }, ...guides().map(guideSidebar)],
    // Read by GuideCards.vue on the home page.
    guides: guides().map(guideCard),
  } as DefaultTheme.Config & { guides: ReturnType<typeof guideCard>[] },
})
