<script lang="ts">
  import { route, viewHref, type Page } from "./lib/router.svelte";
  import About from "./pages/About.svelte";
  import Leaderboard from "./pages/Leaderboard.svelte";
  import Scatter from "./pages/Scatter.svelte";
  import ShotExplorer from "./pages/ShotExplorer.svelte";

  const SHINY_URL = "https://alexsusi2298.shinyapps.io/exp-fg-pct/";
  const NAV: { page: Page; label: string }[] = [
    { page: "about", label: "About" },
    { page: "shot", label: "Shot Explorer" },
    { page: "leaderboard", label: "EPAA Leaderboard" },
    { page: "scatter", label: "EPAA Scatterplot" },
  ];

  // Pages stay mounted after their first visit so their inputs survive tab
  // switches, as they do in the Shiny app.
  let visited = $state<Page[]>([]);
  $effect(() => {
    if (!visited.includes(route.page)) visited = [...visited, route.page];
  });

  const href = (p: Page) => (p === "leaderboard" || p === "scatter" ? viewHref(p, route[p]) : `#/${p}`);
</script>

<header class="nav">
  <div class="nav-inner">
    <a class="brand" href="#/about">Expected Points Above Average</a>
    <nav aria-label="Sections">
      {#each NAV as n (n.page)}
        <a href={href(n.page)} class:active={route.page === n.page} aria-current={route.page === n.page ? "page" : undefined}>{n.label}</a>
      {/each}
    </nav>
  </div>
</header>

<main>
  {#if visited.includes("about")}
    <div hidden={route.page !== "about"}><About /></div>
  {/if}
  {#if visited.includes("shot")}
    <div hidden={route.page !== "shot"}><ShotExplorer /></div>
  {/if}
  {#if visited.includes("leaderboard")}
    <div hidden={route.page !== "leaderboard"}><Leaderboard /></div>
  {/if}
  {#if visited.includes("scatter")}
    <div hidden={route.page !== "scatter"}><Scatter /></div>
  {/if}
  <footer>
    Shot quality, shooting talent and defensive impact from public NBA play-by-play data.
    Code and methodology: <a href="https://github.com/alex-susi/exp-fg-pct" rel="noopener">GitHub repo</a>.
    Original <a href={SHINY_URL} rel="noopener">Shiny dashboard</a>.
  </footer>
</main>

<style>
  .nav {
    position: sticky;
    top: 0;
    z-index: 30;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
  }
  .nav-inner {
    max-width: 1680px;
    margin: 0 auto;
    padding: 0 16px;
    min-height: 48px;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 2px 22px;
  }
  .brand {
    padding: 8px 0;
    font-weight: 800;
    font-size: 1.05rem;
    letter-spacing: -0.01em;
    color: var(--text-strong);
    text-decoration: none;
  }
  nav { display: flex; gap: 2px; overflow-x: auto; scrollbar-width: none; max-width: 100%; }
  nav a {
    padding: 13px 10px;
    color: var(--muted);
    text-decoration: none;
    font-weight: 600;
    white-space: nowrap;
    border-bottom: 2px solid transparent;
  }
  nav a:hover { color: var(--text-strong); }
  nav a.active { color: var(--text-strong); border-bottom-color: var(--text-strong); }
  nav a:focus-visible, .brand:focus-visible { outline: 2px solid var(--link); outline-offset: -2px; }
  main { max-width: 1680px; margin: 0 auto; padding: 12px 16px 24px; }
  footer { margin-top: 20px; font-size: 12px; color: var(--muted); text-align: center; }
  @media (max-width: 560px) {
    .nav-inner { padding: 0 12px; }
    .brand { padding: 8px 0 0; }
    nav a { padding: 9px 7px; font-size: 13px; }
    main { padding: 12px 12px 22px; }
  }
</style>
