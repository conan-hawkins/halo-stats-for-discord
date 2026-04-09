"""History sync planning helpers for full-match coverage checks."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BoundaryProbePlan:
    """Plan for probing one position beyond cached history coverage."""

    required: bool
    start: int
    count: int = 1


@dataclass(frozen=True)
class FullHistorySyncDecision:
    """Decision metadata for incremental full-history validation."""

    boundary_found: bool
    probe_checked: bool
    completeness_proven: bool
    fallback_reason: Optional[str]
    cache_plus_new: int


def build_boundary_probe_plan(
    *,
    full_history_requested: bool,
    has_cached_matches: bool,
    reached_history_end: bool,
    total_matches_hint: Optional[int],
    cached_match_count: int,
    new_match_count: int,
    cache_marked_incomplete: bool = False,
) -> BoundaryProbePlan:
    """
    Determine whether to probe one row past known cache coverage.

    The probe detects the "game 26" scenario where cache contains an initial
    window (for example 25 matches) but older history exists and would be missed
    by stopping at the first cached boundary hit.
    """
    if not full_history_requested:
        return BoundaryProbePlan(required=False, start=0)

    if not has_cached_matches:
        return BoundaryProbePlan(required=False, start=0)

    if total_matches_hint is not None and not cache_marked_incomplete:
        return BoundaryProbePlan(required=False, start=0)

    if reached_history_end and not cache_marked_incomplete:
        return BoundaryProbePlan(required=False, start=0)

    return BoundaryProbePlan(
        required=True,
        start=max(0, int(cached_match_count) + int(new_match_count)),
    )


def decide_full_history_sync(
    *,
    full_history_requested: bool,
    total_matches_hint: Optional[int],
    cached_match_count: int,
    new_match_count: int,
    found_cached_boundary: bool,
    reached_history_end: bool,
    reached_search_cap: bool,
    probe_required: bool,
    probe_checked: bool,
    probe_found_uncached: bool,
    probe_start: int,
    cache_marked_incomplete: bool = False,
) -> FullHistorySyncDecision:
    """Resolve whether incremental sync is complete or must fall back to full fetch."""
    cache_plus_new = max(0, int(cached_match_count) + int(new_match_count))
    boundary_found = bool(found_cached_boundary or reached_history_end)

    if not full_history_requested:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=False,
            completeness_proven=True,
            fallback_reason=None,
            cache_plus_new=cache_plus_new,
        )

    if total_matches_hint is not None and cache_plus_new < total_matches_hint:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=bool(probe_checked),
            completeness_proven=False,
            fallback_reason=(
                "Incremental top-up did not converge to API total count "
                f"(cache+new={cache_plus_new}, api={total_matches_hint}); "
                "falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    if cache_marked_incomplete and not reached_history_end and not found_cached_boundary and not probe_checked:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=False,
            completeness_proven=False,
            fallback_reason=(
                "Cached history is marked incomplete and incremental boundary was not proven; "
                "falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    if reached_search_cap and not boundary_found:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=bool(probe_checked),
            completeness_proven=False,
            fallback_reason=(
                "Incremental top-up reached the search cap before proving completeness; "
                "falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    if probe_required and not probe_checked:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=False,
            completeness_proven=False,
            fallback_reason=(
                "Full-history boundary probe was required but not completed; "
                "falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    if probe_checked and probe_found_uncached:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=True,
            completeness_proven=False,
            fallback_reason=(
                "Boundary probe found uncached older history "
                f"(start={probe_start}); falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    completeness_proven = boundary_found and (not probe_required or (probe_checked and not probe_found_uncached))
    if total_matches_hint is None and not completeness_proven:
        return FullHistorySyncDecision(
            boundary_found=boundary_found,
            probe_checked=bool(probe_checked),
            completeness_proven=False,
            fallback_reason=(
                "Full-history verification missing API total-count metadata "
                "and incremental boundary did not prove completeness; "
                "falling back to full fetch."
            ),
            cache_plus_new=cache_plus_new,
        )

    return FullHistorySyncDecision(
        boundary_found=boundary_found,
        probe_checked=bool(probe_checked),
        completeness_proven=bool(completeness_proven),
        fallback_reason=None,
        cache_plus_new=cache_plus_new,
    )
