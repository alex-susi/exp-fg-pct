<script lang="ts">
  // The EPAA Scatterplot tab: shot quality against estimated talent, with a
  // headshot or logo per actor.
  import ActorTip from "../components/ActorTip.svelte";
  import PlotlyChart from "../components/PlotlyChart.svelte";
  import { loadLbTable } from "../lib/data";
  import { scatterData, scTitle, teamChoices } from "../lib/leaderboard";
  import { replaceView, route } from "../lib/router.svelte";
  import { scatterFigure } from "../lib/scatterCharts";
  import type { LbTable, Metric, Role, ShotFilter } from "../lib/types";

  const MIN_OBS = [0, 50, 100, 200, 300, 500, 750, 1000];

  // Role and shot type live in the address so a view can be shared.
  const role = $derived(route.scatter.role);
  const shot = $derived(route.scatter.shot);
  let metric = $state<Metric>("per100");
  let minObs = $state(200);
  let team = $state("all");
  let teams = $state<string[]>([]);

  let table = $state<LbTable | null>(null);
  let error = $state<string | null>(null);

  $effect(() => {
    const r = role;
    const s = shot;
    loadLbTable(r, s).then((t) => {
      if (r !== route.scatter.role || s !== route.scatter.shot) return;
      // The team menu follows the player tables and is left alone for team
      // defense, where it is hidden. A chosen team is kept while still offered.
      if (r !== "team") {
        const choices = teamChoices(t);
        if (team !== "all" && !choices.includes(team)) team = "all";
        teams = choices;
      }
      table = t;
      error = null;
    }).catch((e: Error) => (error = e.message));
  });

  const rows = $derived(table ? scatterData(table, { metric, minObs, team }) : []);
  const byKey = $derived(new Map(rows.map((r) => [String(r.idx), r])));
  const figure = $derived(scatterFigure(rows, role, metric));
  const title = $derived(scTitle(role, shot, metric));

  const metricText = $derived(metric === "total" ? "total EPAA" : "EPAA per 100");
  const help = $derived({
    offense: `Upper-right players combine harder shot diets with stronger context-adjusted shot-making based on ${metricText}.`,
    defense: `Further-right defenders prevent more expected points based on ${metricText}; higher y-values mean they faced a tougher shot mix.`,
    team: `Further-right teams prevent more expected points based on ${metricText}; higher y-values mean opponents generated a tougher shot mix.`,
  }[role]);

  // Fill the viewport beside the controls on wide screens; a squarer plot on phones.
  const plotHeight = (width: number) =>
    width >= 640 ? Math.max(480, Math.min(window.innerHeight - 150, 900)) : Math.max(360, Math.round(width * 1.05));
</script>

<div class="split">
  <section class="card">
    <h2 class="card-header">Scatterplot Controls</h2>
    <div class="card-body controls">
      <div class="field">
        <label class="field-label" for="sc_role">Show Scatterplot For</label>
        <select id="sc_role" class="select" value={role}
                onchange={(e) => replaceView("scatter", { role: e.currentTarget.value as Role, shot })}>
          <option value="offense">Offensive Players</option>
          <option value="defense">Defensive Players</option>
          <option value="team">Team Defense</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="sc_shot">Shot Type</label>
        <select id="sc_shot" class="select" value={shot}
                onchange={(e) => replaceView("scatter", { role, shot: e.currentTarget.value as ShotFilter })}>
          <option value="all">All Shots</option>
          <option value="j3">3-Pt</option>
          <option value="j2">Mid-Range</option>
          <option value="rim">Rim</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="sc_xaxis">X-Axis Metric</label>
        <select id="sc_xaxis" class="select" bind:value={metric}>
          <option value="total">Total EPAA</option>
          <option value="per100">EPAA per 100 Shots</option>
        </select>
      </div>
      <!-- Hidden, not removed, for team defense, so the choices survive. -->
      <div class="field" hidden={role === "team"}>
        <label class="field-label" for="sc_minobs">Min. Observations</label>
        <select id="sc_minobs" class="select" bind:value={minObs}>
          {#each MIN_OBS as n (n)}<option value={n}>{n}</option>{/each}
        </select>
      </div>
      <div class="field" hidden={role === "team"}>
        <label class="field-label" for="sc_team">Team</label>
        <select id="sc_team" class="select" bind:value={team}>
          <option value="all">All Teams</option>
          {#each teams as t (t)}<option value={t}>{t}</option>{/each}
        </select>
      </div>
      <div class="help">
        <p>{help}</p>
        <p>Hover any headshot or logo for observations, EPAA, and shot quality.</p>
      </div>
    </div>
  </section>

  <section class="card">
    <h2 class="card-header">{title}</h2>
    <div class="card-body plot-body">
      {#if error}
        <p class="prompt error" role="alert">Could not load the scatterplot: {error}</p>
      {:else if !table}
        <p class="prompt">Loading…</p>
      {:else if rows.length === 0}
        <p class="prompt">No players meet the current filters. Try lowering the minimum-observations threshold.</p>
      {:else}
        <PlotlyChart data={figure.data} layout={figure.layout} images={figure.images} height={plotHeight}
                     focusOnHover tipGap={44}
                     label={`${title}: ${rows.length} points, shot quality against talent`}>
          {#snippet tip(key)}
            {@const row = byKey.get(key)}
            {#if row && table}
              <ActorTip {row} obsLabel={table.obs_label} shotQualityUnit="xPts/shot"
                        metricLabel={metric === "total" ? "Total EPAA" : "EPAA per 100"} ciLabel="80%" />
            {/if}
          {/snippet}
        </PlotlyChart>
      {/if}
    </div>
  </section>
</div>

<style>
  .plot-body { padding: 6px 6px 4px; }
</style>
