import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import '@fontsource/jetbrains-mono/400.css'
import '@fontsource/jetbrains-mono/700.css'
import './custom.css'
import GuideCards from './GuideCards.vue'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('GuideCards', GuideCards)
  },
} satisfies Theme
