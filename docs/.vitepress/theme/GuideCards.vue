<script setup lang="ts">
import { useData, withBase } from 'vitepress'

type GuideCard = { title: string; link: string; chapters: number }

const { theme } = useData()
const guides = (theme.value.guides ?? []) as GuideCard[]
</script>

<template>
  <div class="guide-cards">
    <a v-for="g in guides" :key="g.link" class="guide-card" :href="withBase(g.link)">
      <span class="title">{{ g.title }}</span>
      <span class="count">{{ g.chapters }} chapters</span>
    </a>
  </div>
</template>

<style scoped>
.guide-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 16px;
  margin-top: 24px;
}
.guide-card {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 20px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  text-decoration: none;
  transition: border-color 0.2s;
}
.guide-card:hover {
  border-color: var(--vp-c-brand-1);
}
.title {
  font-size: 18px;
  font-weight: 600;
}
.count {
  font-size: 13px;
  color: var(--vp-c-brand-1);
}
</style>
