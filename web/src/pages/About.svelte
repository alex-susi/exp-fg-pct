<script lang="ts">
  // The About tab. The wording lives here so it can be edited without rerunning R.
  const GLOSSARY: [string, string][] = [
    ["Expected Points Above Average (EPAA)", "Expected points added or prevented relative to a league-average player given the shot quality."],
    ["Shot Quality", "The expected points of a shot for a league-average player. Does not consider who took or who defended the shot."],
    ["xFG%", "Expected field goal percentage for a league-average shooter given the shot quality."],
    ["xPts", "Expected points: xFG% multiplied by the number of points scored for the given shot if made."],
    ["Credible Interval", "A posterior uncertainty range from the Bayesian model."],
    ["EPAA per 100", "Normalizes EPAA on a per-100 shot basis, useful for comparing players with different shot volumes."],
    ["Total EPAA", "Volume-weighted EPAA over the observed attempts or shots defended."],
    ["Team Defense", "A team-level defensive effect that captures scheme and context not assigned to a single defender."],
  ];
  const START: [string, string, string][] = [
    ["Shot Explorer", "#/shot", "Choose a shooter, defender, shot location, and shot features to inspect the baseline and player-adjusted shot quality."],
    ["EPAA Leaderboard", "#/leaderboard", "Rank offensive players, defensive players, or team defenses by total EPAA or EPAA per 100."],
    ["EPAA Scatterplot", "#/scatter", "View shot quality against estimated talent."],
  ];
</script>

<div class="about">
  <section class="hero">
    <h1>Expected Points Above Average</h1>
    <p>This dashboard explores EPAA, a two-stage framework for separating shot quality from shot-making talent. First, the app estimates how hard a shot is based on its location and context. Then it layers in Bayesian player, defender, and team random effects to estimate who adds or prevents expected points relative to that baseline.</p>
    <p>Full project details and source code are available in the <a href="https://github.com/alex-susi/exp-fg-pct" target="_blank" rel="noopener noreferrer">GitHub repo</a>.</p>
  </section>

  <div class="row two">
    <section class="card">
      <h2 class="card-header">Shot Quality</h2>
      <div class="card-body">
        <p>Shot quality is the expected value of a shot before crediting the shooter or penalizing the defender. It is estimated from public NBA play-by-play features such as location, shot clock, defender distance, dribbles taken before shooting, and player minutes. The Shot Explorer lets you manually set those inputs and inspect the model's baseline expected field-goal percentage for that exact context.</p>
      </div>
    </section>
    <section class="card">
      <h2 class="card-header">Shooting Talent and Defensive Impact</h2>
      <div class="card-body">
        <p>EPAA measures how much expected value a player adds above the baseline shot quality. Offensive players receive positive credit when their estimated make probability is better than an average shooter facing the same shot mix. Individual defenders and team defenses receive positive credit when they suppress expected points relative to the same baseline.</p>
      </div>
    </section>
  </div>

  <div class="row wide-left">
    <section class="card">
      <h2 class="card-header">Glossary</h2>
      <dl class="card-body glossary">
        {#each GLOSSARY as [term, def] (term)}
          <div><dt>{term}</dt><dd>{def}</dd></div>
        {/each}
      </dl>
    </section>
    <section class="card">
      <h2 class="card-header">Where to Start</h2>
      <dl class="card-body start">
        {#each START as [term, href, def] (term)}
          <div><dt><a {href}>{term}</a></dt><dd>{def}</dd></div>
        {/each}
      </dl>
    </section>
  </div>
</div>

<style>
  .about { max-width: 1180px; margin: 0 auto; padding: 0 0 8px; }
  .hero {
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: linear-gradient(135deg, rgba(109, 40, 217, 0.18), rgba(15, 15, 26, 0.96));
    border-radius: 16px;
    padding: 22px 24px;
    margin-bottom: 14px;
  }
  h1 { margin: 0 0 8px; font-size: 1.75rem; font-weight: 900; letter-spacing: -0.03em; color: var(--text-strong); line-height: 1.15; }
  p { margin: 0; color: var(--text); line-height: 1.65; font-size: 14px; }
  p + p { margin-top: 10px; }
  .row { display: grid; gap: 14px; margin-bottom: 14px; }
  .row.two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .row.wide-left { grid-template-columns: minmax(0, 7fr) minmax(0, 5fr); }
  dl { margin: 0; display: grid; }
  .glossary { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px 18px; }
  .start { grid-template-columns: minmax(0, 1fr); gap: 14px; }
  dt { color: var(--text-strong); font-weight: 800; }
  dt a { color: inherit; text-decoration-color: var(--primary); text-underline-offset: 3px; }
  dt a:hover { color: var(--link-hover); }
  dd { margin: 0; color: #b8bcc9; line-height: 1.55; }
  @media (max-width: 900px) {
    .row.two, .row.wide-left, .glossary { grid-template-columns: minmax(0, 1fr); }
    .hero { padding: 18px 16px; }
    h1 { font-size: 1.45rem; }
  }
</style>
