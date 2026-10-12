<script lang="ts">
  // The Shot Explorer tab: pick a court position, players and shot context, and
  // see the baseline xFG% with the posterior effect of each actor.
  import Court from "../components/Court.svelte";
  import PlayerSearch from "../components/PlayerSearch.svelte";
  import { loadActorDraws, loadCal, loadEcdf, loadMeta, loadModel } from "../lib/data";
  import { fixed, mediaUrl, signed } from "../lib/format";
  import { locateShot, posteriorResults, xfgBaseline, type PosteriorResults, type ShotLocation } from "../lib/shot";
  import type { Meta } from "../lib/types";

  const FAMILY_LABEL = { rim: "Rim", j2: "Mid-Range", j3: "Three-Pointer" };
  const FASTBREAK_MIN_CLOCK = 16;

  let meta = $state<Meta | null>(null);
  let loadError = $state<string | null>(null);
  loadMeta().then((m) => (meta = m)).catch((e: Error) => (loadError = e.message));

  // Inputs, with the defaults of the Shiny app.
  let loc = $state<ShotLocation | null>(null);
  let shooter = $state<number | null>(null);
  let defender = $state<number | null>(null);
  let shotClock = $state(14);
  let dribbleLevel = $state(1);
  let defenderDistLevel = $state(3);
  let minGame = $state(18);
  let minStint = $state(5);
  let isFastbreak = $state(false);
  let isFromTurnover = $state(false);
  let is2ndChance = $state(false);
  let isDunk = $state(false);

  // The defensive team follows the defender (observeEvent(input$defender) in app.R).
  const defteam = $derived(meta && defender ? meta.defenders.team[defender - 1] : null);

  // Input constraints from the server function.
  const stintMax = $derived(Math.min(20, minGame));
  const dunkAllowed = $derived(loc?.family === "rim");
  const fastbreakAllowed = $derived(shotClock >= FASTBREAK_MIN_CLOCK);

  function pickLocation(x: number, y: number) {
    loc = locateShot(x, y);
    if (loc.family !== "rim") isDunk = false; // dunks exist only at the rim
  }
  function setMinGame(v: number) {
    minGame = v;
    minStint = Math.min(minStint, Math.min(20, v)); // a stint cannot outlast the game minutes
  }
  function setShotClock(v: number) {
    shotClock = v;
    if (v < FASTBREAK_MIN_CLOCK) isFastbreak = false; // fast breaks happen early in the clock
  }

  let results = $state<PosteriorResults | null>(null);
  let calcError = $state<string | null>(null);
  let pending = $state(false);
  let run = 0;

  $effect(() => {
    const at = loc;
    const inputs = { minGame, minStint, shotClock, defenderDistLevel, dribbleLevel, is2ndChance, isFastbreak, isFromTurnover, isDunk };
    const s = shooter;
    const d = defender;
    const t = defteam;
    if (!at || !meta) {
      results = null;
      return;
    }
    const id = ++run;
    pending = true;
    Promise.all([
      loadModel(at.family), loadCal(), loadEcdf(),
      s ? loadActorDraws("player", s) : null,
      d ? loadActorDraws("defender", d) : null,
      t ? loadActorDraws("team", t) : null,
    ]).then(([model, cal, ecdf, sDraws, dDraws, tDraws]) => {
      if (id !== run) return;
      results = posteriorResults({
        base: xfgBaseline(model, at, inputs), cal, ecdf,
        shooter: sDraws ? sDraws[at.family] : null,
        defender: dDraws ? dDraws[at.family] : null,
        defteam: tDraws ? tDraws[at.family] : null,
      });
      calcError = null;
      pending = false;
    }).catch((e: Error) => {
      if (id !== run) return;
      calcError = e.message;
      pending = false;
    });
  });

  const effectClass = (v: number) => (Math.abs(v) < 0.3 ? "neutral" : v > 0 ? "positive" : "negative");
  const pct = (v: number) => `${fixed(v, 1)}%`;

  const actorRows = $derived.by(() => {
    const r = results;
    if (!r || !meta) return [];
    const rows: { name: string; median: number; lo: number; hi: number }[] = [];
    if (r.has_shooter && shooter) {
      rows.push({ name: `🏀 Shooter: ${meta.players.name[shooter - 1]}`, median: r.shooter_median, lo: r.shooter_q05, hi: r.shooter_q95 });
    }
    if (r.has_defender && defender) {
      rows.push({ name: `🛡️ Defender: ${meta.defenders.name[defender - 1]}`, median: r.defender_median, lo: r.defender_q05, hi: r.defender_q95 });
    }
    if (r.has_defteam && defteam) {
      rows.push({ name: `🏢 Team Defense: ${meta.teams.abbr[defteam - 1]}`, median: r.defteam_median, lo: r.defteam_q05, hi: r.defteam_q95 });
    }
    return rows;
  });
</script>

{#snippet headshot(name: string, img: string)}
  <div class="headshot">
    {#if img}
      <img src={mediaUrl(img)} alt="" onerror={(e) => ((e.currentTarget as HTMLImageElement).style.display = "none")} />
    {/if}
    <div>{name}</div>
  </div>
{/snippet}

{#snippet slider(id: string, label: string, value: number, max: number, set: (v: number) => void)}
  <div class="cell">
    <div class="slider-head">
      <label class="field-label" for={id}>{label}</label>
      <output for={id}>{value}</output>
    </div>
    <input {id} type="range" min="0" {max} step="1" {value} oninput={(e) => set(Number(e.currentTarget.value))} />
  </div>
{/snippet}

{#if loadError}
  <p class="status error" role="alert">Could not load the model data: {loadError}</p>
{:else if !meta}
  <p class="status">Loading…</p>
{:else}
  <div class="explorer">
    <section class="card players">
      <h2 class="card-header">Players &amp; Team</h2>
      <div class="card-body player-grid">
        <div>
          <PlayerSearch id="shooter" label="Shooter" names={meta.players.name} value={shooter} onchange={(v) => (shooter = v)} />
          {#if shooter}{@render headshot(meta.players.name[shooter - 1], meta.players.img[shooter - 1])}{/if}
        </div>
        <div>
          <PlayerSearch id="defender" label="Primary Defender" names={meta.defenders.name} value={defender} onchange={(v) => (defender = v)} />
          {#if defender}{@render headshot(meta.defenders.name[defender - 1], meta.defenders.img[defender - 1])}{/if}
        </div>
        <div class="team">
          <div class="field-label" id="team-label">Team</div>
          <div class="team-panel" class:empty={!defteam} role="group" aria-labelledby="team-label">
            {#if defteam}
              <div class="team-abbr">{meta.teams.abbr[defteam - 1]}</div>
              {#if meta.teams.logo[defteam - 1]}
                <img src={mediaUrl(meta.teams.logo[defteam - 1])} alt=""
                     onerror={(e) => ((e.currentTarget as HTMLImageElement).style.display = "none")} />
              {/if}
            {:else}
              <span aria-label="No team: choose a defender">—</span>
            {/if}
          </div>
        </div>
      </div>
    </section>

    <section class="card court-card">
      <h2 class="card-header">Click to Set Shot Location</h2>
      <div class="card-body">
        <Court shot={loc} onpick={pickLocation} />
        <div class="shot-info" aria-live="polite">
          {#if loc}
            <span class="badge {loc.family}">{FAMILY_LABEL[loc.family]}</span>
            <span>{fixed(loc.distance, 1)} ft &nbsp;|&nbsp; {fixed(loc.angle, 0)}° from center</span>
          {:else}
            <span class="hint">Click anywhere on the court above</span>
          {/if}
        </div>
      </div>
    </section>

    <section class="card features">
      <h2 class="card-header">Shot Quality Features</h2>
      <div class="card-body feature-grid">
        {@render slider("shot_clock", "Shot Clock (sec)", shotClock, 24, setShotClock)}
        <div class="cell">
          <label class="field-label" for="dribble_range">Dribbles</label>
          <select id="dribble_range" class="select" bind:value={dribbleLevel}>
            {#each meta.dribble_levels as o (o.level)}<option value={o.level}>{o.label}</option>{/each}
          </select>
        </div>
        <div class="cell">
          <label class="field-label" for="defender_dist">Defender Distance</label>
          <select id="defender_dist" class="select" bind:value={defenderDistLevel}>
            {#each meta.defender_dist_levels as o (o.level)}<option value={o.level}>{o.label}</option>{/each}
          </select>
        </div>
        {@render slider("min_game", "Game Min. Played", minGame, 48, setMinGame)}
        {@render slider("min_stint", "Stint Min. Played", minStint, stintMax, (v) => (minStint = v))}
        <fieldset class="cell toggles">
          <legend class="sr-only">Shot situation</legend>
          <label class:disabled={!fastbreakAllowed}>
            <input type="checkbox" bind:checked={isFastbreak} disabled={!fastbreakAllowed} /> Fast Break
          </label>
          <label><input type="checkbox" bind:checked={isFromTurnover} /> Off Turnover</label>
          <label><input type="checkbox" bind:checked={is2ndChance} /> 2nd Chance</label>
          <label class:disabled={!dunkAllowed}>
            <input type="checkbox" bind:checked={isDunk} disabled={!dunkAllowed} /> Dunk
          </label>
        </fieldset>
      </div>
    </section>

    <section class="card results">
      <h2 class="card-header">EPAA Prediction with 90% Credible Intervals</h2>
      <div class="card-body" aria-live="polite" aria-busy={pending}>
        {#if calcError}
          <p class="prompt error" role="alert">Could not compute the prediction: {calcError}</p>
        {:else if !loc}
          <p class="prompt">Select a shot location and players to see predictions</p>
        {:else if !results}
          <p class="prompt">Calculating…</p>
        {:else}
          <div class="hero">
            <div class="stat">
              <div class="result-label">Adjusted xFG%</div>
              <div class="result-value xfg">{pct(results.final_median)}</div>
              <div class="result-ci">[ {pct(results.final_q05)}, {pct(results.final_q95)} ]</div>
            </div>
            <div class="stat">
              <div class="result-label">Expected Points</div>
              <div class="result-value xpts">{fixed(results.xpts_median, 2)}</div>
              <div class="result-ci">[ {fixed(results.xpts_q05, 2)}, {fixed(results.xpts_q95, 2)} ]</div>
            </div>
            <div class="stat grade" style:--grade={results.grade_color}>
              <div class="result-label">Shot Grade</div>
              <div class="grade-circle">{results.letter_grade}</div>
              <div class="grade-number">{results.shot_grade}</div>
              <div class="grade-sublabel">percentile</div>
            </div>
          </div>

          <h3 class="result-label breakdown-title">Effect Breakdown</h3>
          <div class="actor neutral">
            <div>
              <div class="actor-name">xFG</div>
              <div class="actor-ci">League-Average Shot Quality</div>
            </div>
            <div class="actor-right">
              <div class="actor-effect">{pct(results.baseline_median)}</div>
              <div class="actor-ci">[ {pct(results.baseline_q05)}, {pct(results.baseline_q95)} ]</div>
            </div>
          </div>
          {#each actorRows as row (row.name)}
            <div class="actor {effectClass(row.median)}">
              <div class="actor-name">{row.name}</div>
              <div class="actor-right">
                <div class="actor-effect">{signed(row.median)}%</div>
                <div class="actor-ci">[ {signed(row.lo)}, {signed(row.hi)} ]</div>
              </div>
            </div>
          {/each}
        {/if}
      </div>
    </section>
  </div>
{/if}

<style>
  /* Two columns like the Shiny tab: players and court on the left (5/12),
     features and results on the right (7/12). One column on narrow screens. */
  .explorer {
    display: grid;
    grid-template-columns: minmax(0, 5fr) minmax(0, 7fr);
    grid-template-areas: "players features" "court results";
    grid-template-rows: auto 1fr;
    gap: 12px 20px;
    --court-max-height: max(300px, calc(100dvh - 420px));
  }
  .players { grid-area: players; }
  .court-card { grid-area: court; }
  .features { grid-area: features; }
  .results { grid-area: results; }

  .player-grid {
    display: grid;
    grid-template-columns: minmax(0, 5fr) minmax(0, 5fr) minmax(0, 2fr);
    gap: 14px;
    min-height: 152px;
  }
  .headshot { text-align: center; padding: 8px; font-size: 0.8rem; font-weight: 600; color: var(--label); }
  .headshot img {
    display: block; margin: 0 auto 4px;
    width: 90px; height: 66px; object-fit: cover; object-position: top;
    border-radius: 8px; border: 2px solid var(--border); background: var(--bg);
  }
  .team-panel {
    min-height: 108px;
    display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px;
    padding: 8px 10px;
    background: var(--bg); border: 1px solid var(--primary); border-radius: 8px;
  }
  .team-panel.empty { border-color: var(--border); color: var(--faint); }
  .team-abbr { color: var(--primary); font-size: 0.98rem; font-weight: 800; letter-spacing: 0.08em; }
  .team-panel img { width: 58px; height: 58px; object-fit: contain; }

  .court-card .card-body { padding: 8px 10px 10px; }
  .shot-info {
    min-height: 40px; padding-top: 8px;
    display: flex; flex-wrap: wrap; align-items: center; justify-content: center; gap: 6px 16px;
    color: var(--label); font-size: 0.92rem; text-align: center;
  }
  .hint { color: var(--faint); font-style: italic; }
  .badge {
    padding: 4px 12px; border-radius: 20px;
    font-size: 0.74rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.08em;
  }
  .badge.rim { background: var(--orange); color: #fff; }
  .badge.j2 { background: var(--primary); color: #fff; }
  .badge.j3 { background: var(--cyan); color: var(--bg); }

  .feature-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 18px 22px;
    align-items: start;
  }
  .cell { min-width: 0; margin: 0; padding: 0; border: 0; }
  .slider-head { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
  output { color: var(--text-strong); font-weight: 700; font-variant-numeric: tabular-nums; }
  input[type="range"] { width: 100%; height: 28px; margin: 0; accent-color: var(--primary); cursor: pointer; }
  input[type="range"]:focus-visible { outline: 2px solid var(--link); outline-offset: 2px; }
  .toggles { display: grid; grid-template-columns: repeat(2, minmax(0, max-content)); gap: 4px 18px; align-content: center; }
  .toggles label { display: flex; align-items: center; gap: 7px; min-height: 30px; white-space: nowrap; cursor: pointer; }
  .toggles label.disabled { opacity: 0.42; cursor: not-allowed; }
  .toggles input { width: 16px; height: 16px; margin: 0; accent-color: var(--primary); cursor: inherit; }

  .results .card-body { display: flex; flex-direction: column; }
  .hero {
    display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 22px;
    padding: 16px 22px; margin-bottom: 14px;
    border: 1px solid var(--primary); border-radius: 10px; background: var(--surface-2);
  }
  .stat { display: grid; grid-template-rows: auto 1fr auto; align-items: center; min-height: 124px; }
  .result-label {
    margin: 0; color: var(--muted);
    font-size: 0.82rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em;
  }
  .result-value {
    font-size: clamp(1.9rem, 3.1vw, 3.3rem); font-weight: 800; line-height: 1;
    color: var(--text-strong); font-variant-numeric: tabular-nums;
  }
  .result-value.xfg { color: var(--primary); }
  .result-value.xpts { color: var(--cyan); }
  .result-ci { color: var(--muted); font-size: 0.9rem; font-variant-numeric: tabular-nums; }
  .grade { justify-items: center; text-align: center; grid-template-rows: auto 1fr auto auto; }
  .grade-circle {
    width: 64px; height: 64px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    border: 3px solid var(--grade); color: var(--grade);
    font-size: 1.55rem; font-weight: 900;
  }
  .grade-number { margin-top: 6px; color: var(--grade); font-size: 1.2rem; font-weight: 800; line-height: 1; font-variant-numeric: tabular-nums; }
  .grade-sublabel { color: var(--muted); font-size: 0.76rem; }

  .breakdown-title { margin: 0 0 8px; padding-left: 4px; }
  .actor {
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
    padding: 8px 14px; margin-bottom: 6px;
    border-radius: 8px; background: var(--surface-2); border-left: 3px solid var(--muted);
    --effect: var(--muted);
  }
  .actor.positive { border-left-color: var(--good); --effect: var(--good); }
  .actor.negative { border-left-color: var(--bad); --effect: var(--bad); }
  .actor-name { font-size: 0.86rem; font-weight: 700; color: var(--text); min-width: 0; overflow-wrap: anywhere; }
  .actor-right { text-align: right; white-space: nowrap; }
  .actor-effect { font-size: 1rem; font-weight: 800; color: var(--effect); font-variant-numeric: tabular-nums; }
  .actor-ci { font-size: 0.76rem; color: var(--faint); font-variant-numeric: tabular-nums; }

  @media (max-width: 1100px) {
    .feature-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 900px) {
    .explorer {
      grid-template-columns: minmax(0, 1fr);
      grid-template-areas: "players" "court" "features" "results";
      grid-template-rows: none;
      --court-max-height: 70dvh;
    }
    .player-grid { min-height: 0; }
  }
  @media (max-width: 560px) {
    .player-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .team { grid-column: 1 / -1; }
    .team-panel { flex-direction: row; min-height: 56px; }
    .team-panel img { width: 38px; height: 38px; }
    .hero { grid-template-columns: repeat(2, minmax(0, 1fr)); padding: 14px; gap: 14px; }
    .stat { min-height: 0; gap: 6px; }
    .grade { grid-column: 1 / -1; grid-template-rows: none; grid-template-columns: auto auto auto auto; justify-content: center; align-items: center; column-gap: 12px; }
    .grade-number { margin-top: 0; }
  }
  @media (max-width: 420px) {
    .feature-grid { grid-template-columns: minmax(0, 1fr); }
  }
</style>
