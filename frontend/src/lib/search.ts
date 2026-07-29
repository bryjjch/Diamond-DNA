/**
 * Client-side mirror of the API's name matching (`search_players` in
 * `src/api/data_loader.py`). Player names live in the lake as "Last, First", so
 * both sides tokenise before comparing — that is what lets "Ernie Clement",
 * "Clement, Ernie" and "clement" all land on the same player.
 */

const NON_WORD = /[^a-z0-9]+/;

/** Lowercased, accent-folded word tokens: "Acuña Jr., Ronald" → ["acuna", "jr", "ronald"]. */
export function nameTokens(text: string): string[] {
  return text
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .split(NON_WORD)
    .filter(Boolean);
}

/** True when every term is a substring of some token of `name`, in any order. */
export function matchesAllTerms(name: string, terms: string[]): boolean {
  const tokens = nameTokens(name);
  return terms.every((term) => tokens.some((token) => token.includes(term)));
}

/**
 * The single term to send to `/api/search`. The longest one is the most
 * selective, and a lone term matches whatever the server's matching rule is —
 * so the dropdown behaves the same against an API that hasn't picked up the
 * token-wise search fix yet. Remaining terms are applied by `matchesAllTerms`.
 */
export function probeTerm(terms: string[]): string {
  return terms.reduce((longest, term) => (term.length > longest.length ? term : longest), "");
}
