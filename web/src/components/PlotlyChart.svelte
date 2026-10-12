<script lang="ts">
  // Renders one Plotly figure sized to its container. Layout and height are
  // functions of the container width so charts can rearrange on phones.
  //
  // Images are drawn as <img> overlays at data coordinates, and the hover card
  // is HTML, like add_plotly_image_overlays() and add_leaderboard_hover_tooltip()
  // in app.R: the Plotly trace only reports which point is under the pointer.
  import { onDestroy, type Snippet } from "svelte";
  import { loadPlotly, type Layout, type PlotlyLib, type Trace } from "../lib/plotly";
  import type { OverlayImage } from "../lib/types";

  interface Props {
    data: Trace[];
    layout: (width: number) => Layout;
    height: number | ((width: number) => number);
    label: string;
    images?: (width: number) => OverlayImage[];
    // Dim the other images, enlarge the hovered one and draw its interval.
    focusOnHover?: boolean;
    staticPlot?: boolean;
    // Hover card for the point whose customdata is `key`.
    tip?: Snippet<[string]>;
    tipGap?: number;
  }
  let { data, layout, height, label, images, focusOnHover = false, staticPlot = false, tip, tipGap = 14 }: Props = $props();

  interface Placed {
    key: string;
    src: string;
    left: number;
    top: number;
    w: number;
    h: number;
    band: { neg: [number, number] | null; pos: [number, number] | null } | null;
  }

  let el: HTMLDivElement;
  let width = $state(0);
  let plotly = $state<PlotlyLib | null>(null);
  let failed = $state(false);
  let placed = $state<Placed[]>([]);
  // Hovered point: its key and its position in the viewport.
  let hover = $state<{ key: string; x: number; y: number } | null>(null);
  let tipW = $state(0);
  let tipH = $state(0);
  let listening = false;

  loadPlotly().then((p) => (plotly = p)).catch(() => (failed = true));

  const h = $derived(typeof height === "function" ? height(width) : height);

  function placeImages() {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const fl = (el as any)?._fullLayout;
    const xa = fl?.xaxis;
    const ya = fl?.yaxis;
    if (!xa?.l2p || !ya?.l2p) return;
    const [x0, x1] = xa.range;
    const [y0, y1] = ya.range;
    const inside = (v: number, a: number, b: number) => v >= Math.min(a, b) && v <= Math.max(a, b);
    const xPx = (v: number) => xa.l2p(Math.max(Math.min(v, Math.max(x0, x1)), Math.min(x0, x1))) + xa._offset;
    placed = (images?.(width) ?? [])
      .filter((d) => d.src && Number.isFinite(d.y) && inside(d.y, y0, y1)
        && (d.xPx !== undefined || (Number.isFinite(d.x) && inside(d.x!, x0, x1))))
      .map((d) => {
        let band: Placed["band"] = null;
        if (d.lo !== undefined && d.hi !== undefined && Number.isFinite(d.lo) && Number.isFinite(d.hi)) {
          // Split at zero: red below, green above.
          band = {
            neg: d.lo < 0 && Math.min(d.hi, 0) > d.lo ? [xPx(d.lo), xPx(Math.min(d.hi, 0))] : null,
            pos: d.hi > 0 && d.hi > Math.max(d.lo, 0) ? [xPx(Math.max(d.lo, 0)), xPx(d.hi)] : null,
          };
        }
        return {
          key: d.key, src: d.src, w: d.w, h: d.h, band,
          left: (d.xPx !== undefined ? d.xPx : xa.l2p(d.x)) + xa._offset,
          top: ya.l2p(d.y) + ya._offset,
        };
      });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function onHover(ev: any) {
    const p = ev?.points?.[0];
    let cd = p?.customdata;
    if (Array.isArray(cd)) cd = cd[0];
    if (cd == null || !p.xaxis?.l2p || !p.yaxis?.l2p) return;
    const rect = el.getBoundingClientRect();
    hover = {
      key: String(cd),
      x: rect.left + p.xaxis.l2p(p.x) + p.xaxis._offset,
      y: rect.top + p.yaxis.l2p(p.y) + p.yaxis._offset,
    };
  }

  $effect(() => {
    if (!plotly || width === 0) return;
    const config = { displaylogo: false, responsive: false, displayModeBar: false, staticPlot };
    const fig = { ...layout(width), width, height: h };
    void images;
    hover = null;
    plotly.react(el, data, fig, config).then(() => {
      placeImages();
      if (listening || staticPlot) return;
      listening = true;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const gd = el as any;
      gd.on("plotly_relayout", () => setTimeout(placeImages, 25));
      gd.on("plotly_hover", onHover);
      gd.on("plotly_unhover", () => (hover = null));
    });
  });

  // The card is fixed to the viewport, so drop it when anything scrolls.
  $effect(() => {
    if (!hover) return;
    const hide = () => (hover = null);
    window.addEventListener("scroll", hide, { capture: true, passive: true });
    return () => window.removeEventListener("scroll", hide, { capture: true });
  });

  const tipPos = $derived.by(() => {
    if (!hover) return null;
    const above = hover.y - tipH - tipGap > 8;
    const maxLeft = Math.max(8, window.innerWidth - tipW - 8);
    return {
      left: Math.min(Math.max(8, hover.x - tipW / 2), maxLeft),
      top: above ? hover.y - tipH - tipGap : hover.y + tipGap,
    };
  });

  const active = $derived(focusOnHover && hover ? placed.find((d) => d.key === hover!.key) : undefined);

  onDestroy(() => {
    if (plotly && el) plotly.purge(el);
  });
</script>

<div class="plot" bind:clientWidth={width} style:height={`${h}px`} role="figure" aria-label={label}>
  {#if failed}
    <p class="msg">The chart library could not be loaded.</p>
  {:else if !plotly}
    <p class="msg">Loading chart…</p>
  {/if}
  <div bind:this={el}></div>
  {#if active?.band?.neg}
    {@const [from, to] = active.band.neg}
    <div class="band neg" style:left={`${from}px`} style:width={`${to - from}px`} style:top={`${active.top - 2.5}px`}></div>
  {/if}
  {#if active?.band?.pos}
    {@const [from, to] = active.band.pos}
    <div class="band pos" style:left={`${from}px`} style:width={`${to - from}px`} style:top={`${active.top - 2.5}px`}></div>
  {/if}
  {#each placed as d (d.key)}
    <img class="overlay" src={d.src} alt="" loading="lazy" decoding="async"
         class:active={focusOnHover && hover?.key === d.key}
         class:dimmed={focusOnHover && hover !== null && hover.key !== d.key}
         style:left={`${d.left - d.w / 2}px`} style:top={`${d.top - d.h / 2}px`}
         style:width={`${d.w}px`} style:height={`${d.h}px`}
         onerror={(e) => ((e.currentTarget as HTMLImageElement).style.visibility = "hidden")} />
  {/each}
</div>

{#if hover && tip && tipPos}
  <div class="tip" role="tooltip" bind:offsetWidth={tipW} bind:offsetHeight={tipH}
       style:left={`${tipPos.left}px`} style:top={`${tipPos.top}px`}
       style:visibility={tipW ? "visible" : "hidden"}>
    {@render tip(hover.key)}
  </div>
{/if}

<style>
  .plot { position: relative; width: 100%; min-width: 0; }
  .msg { position: absolute; inset: 0; display: grid; place-items: center; margin: 0; color: var(--muted); }
  .overlay {
    position: absolute;
    object-fit: contain;
    pointer-events: none;
    z-index: 2;
    transition: opacity 90ms linear, transform 90ms ease-out;
  }
  .overlay.active { z-index: 4; transform: scale(1.75); }
  .overlay.dimmed { opacity: 0.12; filter: grayscale(1); }
  .band { position: absolute; height: 5px; pointer-events: none; z-index: 3; opacity: 0.98; }
  .band.neg { background: var(--bad); }
  .band.pos { background: var(--good); }
  .tip {
    position: fixed;
    z-index: 1000;
    pointer-events: none;
    max-width: min(300px, calc(100vw - 16px));
    padding: 8px 10px;
    border: 1px solid var(--primary);
    background: var(--bg);
    color: var(--heading);
    font-size: 12px;
    font-weight: 500;
    line-height: 1.3;
    box-shadow: 0 12px 22px rgba(0, 0, 0, 0.45);
  }
  @media (prefers-reduced-motion: reduce) {
    .overlay { transition: none; }
  }
</style>
