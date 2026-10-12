<script lang="ts">
  // Hover card shared by the leaderboard and the scatterplot (df$hover in app.R).
  import { ciBandPositivePct } from "../lib/leaderboard";
  import { count, fixed, mediaUrl, signed } from "../lib/format";
  import type { LbRow } from "../lib/types";

  interface Props {
    row: LbRow;
    obsLabel: string;
    metricLabel: string;
    ciLabel: string;
    showImage?: boolean;
    shotQualityUnit: string;
  }
  let { row, obsLabel, metricLabel, ciLabel, showImage = false, shotQualityUnit }: Props = $props();
</script>

{#if showImage && row.img_url}
  <div class="img"><img src={mediaUrl(row.img_url)} alt="" /></div>
{/if}
<div class="head">
  <b>{row.label}</b>
  {#if row.team_abbr && row.team_abbr !== row.label}<span>{row.team_abbr}</span>{/if}
</div>
<table>
  <tbody>
    <tr><td>{obsLabel}</td><td>{count(row.n_obs)}</td></tr>
    <tr><td>{metricLabel}</td><td>{signed(row.mid)}</td></tr>
    <tr><td>{ciLabel} CI</td><td>[{signed(row.lo)}, {signed(row.hi)}]</td></tr>
    <tr><td>P(Above Average)</td><td>{fixed(ciBandPositivePct(row.lo, row.hi), 1)}%</td></tr>
    <tr><td>Shot Quality</td><td>{fixed(row.shot_quality, 2)} {shotQualityUnit}</td></tr>
  </tbody>
</table>

<style>
  .img { text-align: center; margin-bottom: 6px; }
  .img img { width: 62px; height: 46px; object-fit: contain; object-position: top; }
  .head { display: flex; justify-content: space-between; gap: 30px; min-width: 210px; margin-bottom: 8px; }
  table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
  td { padding: 0; }
  td:first-child { padding-right: 18px; }
  td:last-child { text-align: right; white-space: nowrap; }
</style>
