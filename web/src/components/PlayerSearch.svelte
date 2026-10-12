<script lang="ts">
  // Type-to-search picker for shooters and defenders. Like the selectize inputs
  // in app.R, the list opens only once something is typed and shows ten names.
  interface Props {
    id: string;
    label: string;
    names: string[]; // index i is the actor with app.R index i + 1
    value: number | null;
    onchange: (idx: number | null) => void;
  }
  let { id, label, names, value, onchange }: Props = $props();

  const MAX_OPTIONS = 10;
  const fold = (s: string) => s.normalize("NFD").replace(/\p{M}/gu, "").toLowerCase();
  const folded = $derived(names.map(fold));

  let query = $state("");
  let editing = $state(false);
  let active = $state(0);
  let input: HTMLInputElement;

  const text = $derived(editing ? query : value ? names[value - 1] : "");

  // Every typed word must appear in the name; earlier matches rank higher.
  const matches = $derived.by(() => {
    const words = fold(query).split(/\s+/).filter(Boolean);
    if (!editing || !words.length) return [];
    const scored: { idx: number; score: number }[] = [];
    folded.forEach((name, i) => {
      let score = 0;
      for (const w of words) {
        const at = name.indexOf(w);
        if (at < 0) return;
        score += at === 0 || name[at - 1] === " " ? 0 : 1 + at / 100;
      }
      scored.push({ idx: i + 1, score });
    });
    return scored.sort((a, b) => a.score - b.score || a.idx - b.idx).slice(0, MAX_OPTIONS);
  });
  const open = $derived(matches.length > 0);

  function choose(idx: number | null) {
    editing = false;
    query = "";
    if (idx !== value) onchange(idx);
  }

  function oninput(e: Event) {
    query = (e.currentTarget as HTMLInputElement).value;
    editing = true;
    active = 0;
    // Emptying the box clears the selection, as backspace does in selectize.
    if (query === "" && value !== null) onchange(null);
  }

  function onkeydown(e: KeyboardEvent) {
    if (e.key === "Escape") {
      editing = false;
      query = "";
    } else if (open && e.key === "ArrowDown") {
      e.preventDefault();
      active = (active + 1) % matches.length;
    } else if (open && e.key === "ArrowUp") {
      e.preventDefault();
      active = (active - 1 + matches.length) % matches.length;
    } else if (open && e.key === "Enter") {
      e.preventDefault();
      choose(matches[active].idx);
    }
  }
</script>

<div class="search">
  <label class="field-label" for={id}>{label}</label>
  <div class="box">
    <input bind:this={input} {id} class="input" type="text" role="combobox" autocomplete="off" spellcheck="false"
           placeholder="Type to search..." value={text} {oninput} {onkeydown}
           onblur={() => { editing = false; query = ""; }}
           aria-autocomplete="list" aria-expanded={open} aria-controls={`${id}-list`}
           aria-activedescendant={open ? `${id}-opt-${matches[active].idx}` : undefined} />
    {#if value !== null}
      <button type="button" class="clear" aria-label={`Clear ${label.toLowerCase()}`}
              onclick={() => { choose(null); input.focus(); }}>×</button>
    {/if}
    <ul id={`${id}-list`} role="listbox" aria-label={label} hidden={!open}>
      {#each matches as m, i (m.idx)}
        <li id={`${id}-opt-${m.idx}`} role="option" aria-selected={i === active} class:active={i === active}
            onpointerdown={(e) => { e.preventDefault(); choose(m.idx); }}
            onpointerenter={() => (active = i)}>
          {names[m.idx - 1]}
        </li>
      {/each}
    </ul>
  </div>
</div>

<style>
  .search { min-width: 0; }
  .box { position: relative; }
  .input { padding-right: 34px; }
  .clear {
    position: absolute; top: 1px; right: 1px; bottom: 1px; width: 32px;
    border: 0; border-radius: 0 7px 7px 0; background: transparent;
    color: var(--muted); font-size: 18px; line-height: 1; cursor: pointer;
  }
  .clear:hover { color: var(--text-strong); }
  .clear:focus-visible { outline: 2px solid var(--link); outline-offset: -2px; }
  ul {
    position: absolute; z-index: 20; left: 0; right: 0; top: calc(100% + 4px);
    margin: 0; padding: 4px 0; list-style: none;
    max-height: 280px; overflow-y: auto;
    background: var(--surface); border: 1px solid var(--border-strong); border-radius: 8px;
    box-shadow: 0 12px 22px rgba(0, 0, 0, 0.45);
  }
  li { padding: 8px 12px; color: var(--text); cursor: pointer; }
  li.active { background: rgba(109, 40, 217, 0.32); color: var(--text-strong); }
</style>
