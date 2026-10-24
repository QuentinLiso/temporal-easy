import { defineConfig } from 'vitepress'

const workflows = {
  text: 'Workflows',
  items: [
    { text: 'Overview', link: '/workflows/docs/README' },
    { text: '01 · What is a Workflow?', link: '/workflows/docs/01-what-is-a-workflow' },
    { text: '02 · Defining Workflows', link: '/workflows/docs/02-defining-workflows' },
    { text: '03 · Determinism and replay', link: '/workflows/docs/03-determinism-and-replay' },
    { text: '04 · Executions, IDs and limits', link: '/workflows/docs/04-workflow-execution-and-ids' },
    { text: '05 · Timers, Selectors, concurrency', link: '/workflows/docs/05-timers-selectors-concurrency' },
    { text: '06 · Message passing', link: '/workflows/docs/06-message-passing' },
    { text: '07 · Child Workflows', link: '/workflows/docs/07-child-workflows' },
    { text: '08 · Cancel, Terminate, Pause, Reset', link: '/workflows/docs/08-cancel-terminate-pause-reset' },
    { text: '09 · Continue-As-New', link: '/workflows/docs/09-continue-as-new' },
    { text: '10 · Schedules and Cron', link: '/workflows/docs/10-schedules-and-cron' },
    { text: '11 · Versioning', link: '/workflows/docs/11-versioning' },
    { text: '12 · Dynamic Workflows', link: '/workflows/docs/12-dynamic-workflows' },
    { text: '13 · Workflow Streams', link: '/workflows/docs/13-workflow-streams' },
  ],
}

const activities = {
  text: 'Activities',
  items: [
    { text: 'Overview', link: '/activities/docs/README' },
    { text: '01 · What is an Activity?', link: '/activities/docs/01-what-is-an-activity' },
    { text: '02 · Defining Activities', link: '/activities/docs/02-defining-activities' },
    { text: '03 · Calling Activities', link: '/activities/docs/03-workflow-activities' },
    { text: '04 · Timeouts, retries, heartbeats', link: '/activities/docs/04-timeouts-retries-heartbeats' },
    { text: '05 · Failures and cancellation', link: '/activities/docs/05-failures-and-cancellation' },
    { text: '06 · Standalone Activities', link: '/activities/docs/06-standalone-activities' },
    { text: '07 · Local Activities', link: '/activities/docs/07-local-activities' },
    { text: '08 · Async completion', link: '/activities/docs/08-async-completion' },
    { text: '09 · Dynamic Activities', link: '/activities/docs/09-dynamic-activities' },
    { text: '10 · Activity operations', link: '/activities/docs/10-activity-operations' },
  ],
}

export default defineConfig({
  title: 'Temporal with Go',
  description: 'A progressive roadmap to Temporal Workflows and Activities in Go.',
  cleanUrls: true,
  srcExclude: ['**/node_modules/**'],
  // Temporal Web UI links (localhost:8233) are intentional.
  ignoreDeadLinks: 'localhostLinks',
  themeConfig: {
    search: { provider: 'local' },
    outline: [2, 3],
    nav: [
      { text: 'Workflows', link: '/workflows/docs/README' },
      { text: 'Activities', link: '/activities/docs/README' },
    ],
    sidebar: {
      '/workflows/': [workflows, { text: 'See also', items: [{ text: 'Activities guide', link: '/activities/docs/README' }] }],
      '/activities/': [activities, { text: 'See also', items: [{ text: 'Workflows guide', link: '/workflows/docs/README' }] }],
    },
  },
})
