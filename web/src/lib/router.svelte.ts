// Hash routes, so every view works as a static page on GitHub Pages:
//   #/about   #/shot   #/leaderboard/<role>/<shot>   #/scatter/<role>/<shot>
import type { Role, ShotFilter } from "./types";

export const PAGES = ["about", "shot", "leaderboard", "scatter"] as const;
export type Page = (typeof PAGES)[number];

const ROLES: Role[] = ["offense", "defense", "team"];
const SHOTS: ShotFilter[] = ["all", "j3", "j2", "rim"];

export interface View {
  role: Role;
  shot: ShotFilter;
}

interface Route {
  page: Page;
  // Each chart tab keeps its own view, so it survives visits to other tabs.
  leaderboard: View;
  scatter: View;
}

function parse(prev?: Route): Route {
  const [page, role, shot] = location.hash.replace(/^#\/?/, "").split("/");
  const next: Route = {
    page: (PAGES as readonly string[]).includes(page) ? (page as Page) : "about",
    leaderboard: prev?.leaderboard ?? { role: "offense", shot: "all" },
    scatter: prev?.scatter ?? { role: "offense", shot: "all" },
  };
  if (next.page === "leaderboard" || next.page === "scatter") {
    const current = next[next.page];
    next[next.page] = {
      role: ROLES.includes(role as Role) ? (role as Role) : current.role,
      shot: SHOTS.includes(shot as ShotFilter) ? (shot as ShotFilter) : current.shot,
    };
  }
  return next;
}

export const route = $state<Route>(parse());

window.addEventListener("hashchange", () => {
  const pageChanged = parse(route).page !== route.page;
  Object.assign(route, parse(route));
  if (pageChanged) window.scrollTo(0, 0);
});

export const viewHref = (page: "leaderboard" | "scatter", view: View) => `#/${page}/${view.role}/${view.shot}`;

// Mirror a menu choice in the address without adding a history entry, so a
// view can be shared as a link.
export function replaceView(page: "leaderboard" | "scatter", view: View) {
  history.replaceState(null, "", viewHref(page, view));
  Object.assign(route, parse(route));
}
