<script lang="ts">
  // The half court from draw_court() in app.R, drawn in feet with the hoop at
  // the origin. A click (or the arrow keys) reports a court position.
  import { COURT, type ShotLocation } from "../lib/shot";

  interface Props {
    shot: ShotLocation | null;
    onpick: (x: number, y: number) => void;
  }
  let { shot, onpick }: Props = $props();

  let svg: SVGSVGElement;

  // The plotted area of draw_court(), with y flipped for SVG.
  const VIEW = { x: -25.15, y: -41.95, w: 50.3, h: 47.3 };

  const pts = (xy: [number, number][]) => xy.map(([x, y]) => `${x.toFixed(3)},${(-y).toFixed(3)}`).join(" ");
  const arc = (from: number, to: number, n: number, f: (a: number) => [number, number]) =>
    pts(Array.from({ length: n }, (_, i) => f(from + ((to - from) * i) / (n - 1))));

  const deg = Math.PI / 180;
  const threeArc = arc(-68 * deg, 68 * deg, 200, (a) => [23.75 * Math.sin(a), 23.75 * Math.cos(a)]);
  const threeY = 23.75 * Math.cos(68 * deg);
  const ftCircle = arc(0, 2 * Math.PI, 100, (a) => [6 * Math.cos(a), 6 * Math.sin(a) + 13.75]);
  const restricted = arc(0, Math.PI, 50, (a) => [4 * Math.cos(a), 4 * Math.sin(a)]);
  const centerCircle = arc(Math.PI, 2 * Math.PI, 50, (a) => [6 * Math.cos(a), 6 * Math.sin(a) + 41.75]);

  const MARKER = { rim: "#FF6B35", j2: "#FFD700", j3: "#00D4FF" };

  function onclick(e: MouseEvent) {
    if (e.detail === 0) return; // keyboard activation carries no position
    // The drawing is scaled to fit and centered in the element (xMidYMid meet).
    const r = svg.getBoundingClientRect();
    const scale = Math.min(r.width / VIEW.w, r.height / VIEW.h);
    if (!(scale > 0)) return;
    const left = r.left + (r.width - VIEW.w * scale) / 2;
    const top = r.top + (r.height - VIEW.h * scale) / 2;
    onpick(VIEW.x + (e.clientX - left) / scale, -(VIEW.y + (e.clientY - top) / scale));
  }

  function onkeydown(e: KeyboardEvent) {
    const step = e.shiftKey ? 0.25 : 1;
    const move: Record<string, [number, number]> = {
      ArrowLeft: [-step, 0], ArrowRight: [step, 0], ArrowUp: [0, step], ArrowDown: [0, -step],
    };
    const d = move[e.key];
    if (!d) return;
    e.preventDefault();
    // The first key press drops the marker at the free-throw area.
    if (shot) onpick(shot.x + d[0], shot.y + d[1]);
    else onpick(0, 10);
  }
</script>

<button type="button" class="court" {onclick} {onkeydown}
        aria-label="Half court. Click to set the shot location, or move it one foot at a time with the arrow keys.">
  <svg bind:this={svg} viewBox={`${VIEW.x} ${VIEW.y} ${VIEW.w} ${VIEW.h}`} preserveAspectRatio="xMidYMid meet" aria-hidden="true">
    <rect x={COURT.xMin} y={-COURT.yMax} width="50" height="47" fill="#dfbb85" />
    <rect class="line" x="-8" y="-13.75" width="16" height="19" fill="#c9a870" />
    <line class="line heavy" x1="-3" x2="3" y1="1.25" y2="1.25" />
    <circle class="rim" cx="0" cy="0" r="0.75" />
    <polyline class="line" points={restricted} />
    <polyline class="line thin dashed" points={ftCircle} />
    <polyline class="line" points={threeArc} />
    <line class="line" x1="-22" x2="-22" y1="5.25" y2={-threeY} />
    <line class="line" x1="22" x2="22" y1="5.25" y2={-threeY} />
    <rect class="line border" x={COURT.xMin} y={-COURT.yMax} width="50" height="47" fill="none" />
    <line class="line heavy" x1="-25" x2="25" y1="5.25" y2="5.25" />
    <polyline class="line" points={centerCircle} />
    {#if shot}
      <line class="trail" x1={shot.x} y1={-shot.y} x2="0" y2="0" />
      <circle class="marker" cx={shot.x} cy={-shot.y} r="0.95" fill={MARKER[shot.family]} />
    {/if}
  </svg>
</button>

<style>
  .court {
    display: block;
    width: 100%;
    padding: 0;
    border: 0;
    border-radius: 6px;
    background: transparent;
    cursor: crosshair;
    touch-action: manipulation;
  }
  .court:focus-visible { outline: 2px solid var(--link); outline-offset: 3px; }
  svg { display: block; width: 100%; height: auto; max-height: var(--court-max-height, 64dvh); }
  .line { fill: none; stroke: #fff; stroke-width: 1.5px; vector-effect: non-scaling-stroke; }
  rect.line { fill: #c9a870; }
  rect.line.border { fill: none; stroke-width: 2px; }
  .heavy { stroke-width: 2.5px; }
  .thin { stroke-width: 1px; }
  .dashed { stroke-dasharray: 5 4; }
  .rim { fill: none; stroke: #ff6b35; stroke-width: 2px; vector-effect: non-scaling-stroke; }
  .trail {
    stroke: #fff; stroke-opacity: 0.5; stroke-width: 1px; stroke-dasharray: 1 3;
    stroke-linecap: round; vector-effect: non-scaling-stroke;
  }
  .marker { stroke: #fff; stroke-width: 2px; vector-effect: non-scaling-stroke; }
</style>
