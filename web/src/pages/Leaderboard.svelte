<script lang="ts">
  // The EPAA Leaderboard tab: a caterpillar plot of posterior means and
  // credible intervals under a fixed axis, filtered and sorted in the browser.
  import ActorTip from "../components/ActorTip.svelte";
  import PlotlyChart from "../components/PlotlyChart.svelte";
  import { loadLbTable } from "../lib/data";
  import { lbData, lbTitle, teamChoices } from "../lib/leaderboard";
  import { LB_AXIS_HEIGHT, lbAxisFigure, lbBodyFigure, lbPlotHeight } from "../lib/leaderboardCharts";
  import { replaceView, route } from "../lib/router.svelte";
  import type { CiKey, LbTable, Metric, Role, ShotFilter, SortKey } from "../lib/types";

  const MIN_OBS = [0, 50, 100, 200, 300, 500, 750, 1000];
  const HELP: Record<Role, string> = {
    offense: "Higher EPAA = more expected points added above a league-average shooter on the same shots.",
    defense: "Higher EPAA = more expected points prevented below what an average defender would allow.",
    team: "Higher EPAA = more expected points prevented by the team's defensive scheme.",
  };

  // Role and shot type live in the address so a view can be shared.
  const role = $derived(route.leaderboard.role);
  const shot = $derived(route.leaderboard.shot);
  let metric = $state<Metric>("total");
  let ci = $state<CiKey>("80");
  let sort = $state<SortKey>("epaa_desc");
  let minObs = $state(0);
  let team = $state("all");
  let teams = $state<string[]>([]);

  let table = $state<LbTable | null>(null);
  let error = $state<string | null>(null);

  $effect(() => {
    const r = role;
    const s = shot;
    loadLbTable(r, s).then((t) => {
      if (r !== route.leaderboard.role || s !== route.leaderboard.shot) return;
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

  const rows = $derived(table ? lbData(table, { metric, ci, sort, minObs, team }) : []);
  const byKey = $derived(new Map(rows.map((r) => [String(r.idx), r])));
  const axis = $derived(lbAxisFigure(rows, metric));
  const body = $derived(lbBodyFigure(rows));
  const title = $derived(lbTitle(role, shot, metric));
</script>

<div class="split">
  <section class="card">
    <h2 class="card-header">Leaderboard Controls</h2>
    <div class="card-body controls">
      <div class="field">
        <label class="field-label" for="lb_role">Show Leaderboard For</label>
        <select id="lb_role" class="select" value={role}
                onchange={(e) => replaceView("leaderboard", { role: e.currentTarget.value as Role, shot })}>
          <option value="offense">Offensive Players</option>
          <option value="defense">Defensive Players</option>
          <option value="team">Team Defense</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="lb_shot">Shot Type</label>
        <select id="lb_shot" class="select" value={shot}
                onchange={(e) => replaceView("leaderboard", { role, shot: e.currentTarget.value as ShotFilter })}>
          <option value="all">All Shots</option>
          <option value="j3">3-Pt</option>
          <option value="j2">Mid-Range</option>
          <option value="rim">Rim</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="lb_xaxis">X-Axis Metric</label>
        <select id="lb_xaxis" class="select" bind:value={metric}>
          <option value="total">Total EPAA</option>
          <option value="per100">EPAA per 100 Shots</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="lb_ci">Credible Interval</label>
        <select id="lb_ci" class="select" bind:value={ci}>
          <option value="80">80% [10%, 90%]</option>
          <option value="50">50% [25%, 75%]</option>
          <option value="20">20% [40%, 60%]</option>
        </select>
      </div>
      <div class="field">
        <label class="field-label" for="lb_sort">Sort By</label>
        <select id="lb_sort" class="select" bind:value={sort}>
          <option value="epaa_desc">EPAA: High to Low</option>
          <option value="epaa_asc">EPAA: Low to High</option>
          <option value="sq_desc">Shot Quality: High to Low</option>
          <option value="sq_asc">Shot Quality: Low to High</option>
        </select>
      </div>
      <!-- Hidden, not removed, for team defense, so the choices survive. -->
      <div class="field" hidden={role === "team"}>
        <label class="field-label" for="lb_minobs">Min. Observations</label>
        <select id="lb_minobs" class="select" bind:value={minObs}>
          {#each MIN_OBS as n (n)}<option value={n}>{n}</option>{/each}
        </select>
      </div>
      <div class="field" hidden={role === "team"}>
        <label class="field-label" for="lb_team">Team</label>
        <select id="lb_team" class="select" bind:value={team}>
          <option value="all">All Teams</option>
          {#each teams as t (t)}<option value={t}>{t}</option>{/each}
        </select>
      </div>
      <div class="help">
        <p>{HELP[role]}</p>
        <p>Point = posterior mean; bars = credible interval; dashed line = league average (0).</p>
      </div>
    </div>
  </section>

  <section class="card">
    <h2 class="card-header">{title}</h2>
    <div class="card-body plot-body">
      {#if error}
        <p class="prompt error" role="alert">Could not load the leaderboard: {error}</p>
      {:else if !table}
        <p class="prompt">Loading…</p>
      {:else if rows.length === 0}
        <p class="prompt">No players meet the current filters. Try lowering the minimum-observations threshold.</p>
      {:else}
        <!-- The axis sits inside the scroll box so both charts share one width. -->
        <div class="scroll">
          <div class="axis">
            <PlotlyChart data={axis.data} layout={axis.layout} height={LB_AXIS_HEIGHT} staticPlot
                         label={`Axis: ${metric === "per100" ? "EPAA per 100 shots" : "total EPAA"}`} />
          </div>
          <PlotlyChart data={body.data} layout={body.layout} images={body.images} height={lbPlotHeight(rows.length)}
                       label={`${title}: ${rows.length} rows with posterior mean and ${ci}% credible interval`}>
            {#snippet tip(key)}
              {@const row = byKey.get(key)}
              {#if row && table}
                <ActorTip {row} obsLabel={table.obs_label} showImage shotQualityUnit="XPts/shot"
                          metricLabel={metric === "total" ? "Total EPAA" : "EPAA per 100"} ciLabel={`${ci}%`} />
              {/if}
            {/snippet}
          </PlotlyChart>
        </div>
      {/if}
    </div>
  </section>
</div>

<style>
  .plot-body { padding: 0; }
  .scroll {
    height: max(420px, calc(100dvh - 150px));
    overflow-y: auto;
    overflow-x: hidden;
    border-radius: 0 0 var(--radius) var(--radius);
  }
  .axis {
    position: sticky;
    top: 0;
    z-index: 5;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
  }
  @media (max-width: 900px) {
    .scroll { height: 72dvh; }
  }
</style>
