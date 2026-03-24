import { useCallback, useRef, useState } from 'react'
import { api, type AmbiguousTerm, type ParsedFilters, type SearchResponse, type Tower } from '@/lib/api'

export type SearchPhase = 'idle' | 'loading' | 'disambiguation' | 'results' | 'error'

export interface TowerSearchState {
  phase: SearchPhase
  query: string
  parsedFilters: ParsedFilters | null
  ambiguous: AmbiguousTerm[]
  results: Tower[]
  total: number
  page: number
  pages: number
  sortBy: string
  sortOrder: 'asc' | 'desc'
  error: string | null
  suggestions: string[]
  showSuggestions: boolean
}

const INITIAL: TowerSearchState = {
  phase: 'idle',
  query: '',
  parsedFilters: null,
  ambiguous: [],
  results: [],
  total: 0,
  page: 1,
  pages: 1,
  sortBy: 'state',
  sortOrder: 'asc',
  error: null,
  suggestions: [],
  showSuggestions: false,
}

export function useTowerSearch() {
  const [state, setState] = useState<TowerSearchState>(INITIAL)
  const suggestTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const _setLoading = () =>
    setState(s => ({ ...s, phase: 'loading', error: null, showSuggestions: false }))

  const _handleResponse = (resp: SearchResponse, newPage: number, sortBy: string, sortOrder: 'asc' | 'desc') => {
    if (resp.ambiguous.length > 0) {
      setState(s => ({
        ...s,
        phase: 'disambiguation',
        parsedFilters: resp.parsed,
        ambiguous: resp.ambiguous,
        results: [],
        total: 0,
      }))
    } else {
      setState(s => ({
        ...s,
        phase: 'results',
        parsedFilters: resp.parsed,
        ambiguous: [],
        results: resp.results,
        total: resp.total,
        page: newPage,
        pages: resp.pages,
        sortBy,
        sortOrder,
      }))
    }
  }

  const search = useCallback(async (query: string, resolved?: Record<string, string>) => {
    if (!query.trim()) {
      setState(INITIAL)
      return
    }
    _setLoading()
    setState(s => ({ ...s, query }))
    try {
      const resp = await api.search({
        query,
        page: 1,
        per_page: 50,
        sort_by: state.sortBy,
        sort_order: state.sortOrder,
        resolved: resolved ?? {},
      })
      _handleResponse(resp, 1, state.sortBy, state.sortOrder)
    } catch (e: unknown) {
      setState(s => ({ ...s, phase: 'error', error: (e as Error).message }))
    }
  }, [state.sortBy, state.sortOrder])

  const resolveDisambiguation = useCallback(
    async (_term: AmbiguousTerm, option: { city: string; state: string }) => {
      _setLoading()
      try {
        const resp = await api.search({
          query: state.query,
          page: 1,
          per_page: 50,
          sort_by: state.sortBy,
          sort_order: state.sortOrder,
          resolved: { city: option.city, state: option.state },
        })
        _handleResponse(resp, 1, state.sortBy, state.sortOrder)
      } catch (e: unknown) {
        setState(s => ({ ...s, phase: 'error', error: (e as Error).message }))
      }
    },
    [state.query, state.sortBy, state.sortOrder]
  )

  const changePage = useCallback(
    async (newPage: number) => {
      if (!state.parsedFilters) return
      _setLoading()
      try {
        const resp = await api.search({
          query: state.query,
          page: newPage,
          per_page: 50,
          sort_by: state.sortBy,
          sort_order: state.sortOrder,
          resolved: {
            ...(state.parsedFilters.city ? { city: state.parsedFilters.city } : {}),
            ...(state.parsedFilters.state ? { state: state.parsedFilters.state } : {}),
          },
        })
        _handleResponse(resp, newPage, state.sortBy, state.sortOrder)
      } catch (e: unknown) {
        setState(s => ({ ...s, phase: 'error', error: (e as Error).message }))
      }
    },
    [state.query, state.parsedFilters, state.sortBy, state.sortOrder]
  )

  const changeSort = useCallback(
    async (sortBy: string, sortOrder: 'asc' | 'desc') => {
      if (!state.parsedFilters) return
      _setLoading()
      try {
        const resp = await api.search({
          query: state.query,
          page: 1,
          per_page: 50,
          sort_by: sortBy,
          sort_order: sortOrder,
          resolved: {
            ...(state.parsedFilters.city ? { city: state.parsedFilters.city } : {}),
            ...(state.parsedFilters.state ? { state: state.parsedFilters.state } : {}),
          },
        })
        _handleResponse(resp, 1, sortBy, sortOrder)
      } catch (e: unknown) {
        setState(s => ({ ...s, phase: 'error', error: (e as Error).message }))
      }
    },
    [state.query, state.parsedFilters]
  )

  const removeFilter = useCallback(
    async (field: keyof ParsedFilters) => {
      if (!state.parsedFilters) return
      const newFilters = { ...state.parsedFilters, [field]: undefined }
      // Rebuild query from remaining filters and re-search
      const parts: string[] = []
      if (newFilters.generation) parts.push(newFilters.generation)
      if (newFilters.site_type) parts.push(newFilters.site_type)
      if (newFilters.city) parts.push(newFilters.city)
      if (newFilters.state) parts.push(newFilters.state)
      if (newFilters.active === true) parts.push('active')
      if (newFilters.active === false) parts.push('inactive')
      if (newFilters.rural === true) parts.push('rural')
      if (newFilters.rural === false) parts.push('urban')
      const newQuery = parts.join(' ')
      if (!newQuery) {
        setState(INITIAL)
        return
      }
      await search(newQuery)
    },
    [state.parsedFilters, search]
  )

  const updateSuggestions = useCallback((q: string) => {
    if (suggestTimer.current) clearTimeout(suggestTimer.current)
    if (!q.trim() || q.length < 2) {
      setState(s => ({ ...s, suggestions: [], showSuggestions: false }))
      return
    }
    suggestTimer.current = setTimeout(async () => {
      try {
        const { suggestions } = await api.suggest(q)
        setState(s => ({ ...s, suggestions, showSuggestions: suggestions.length > 0 }))
      } catch {
        // silent
      }
    }, 250)
  }, [])

  const hideSuggestions = useCallback(() => {
    setState(s => ({ ...s, showSuggestions: false }))
  }, [])

  const clear = useCallback(() => setState(INITIAL), [])

  return {
    state,
    search,
    resolveDisambiguation,
    changePage,
    changeSort,
    removeFilter,
    updateSuggestions,
    hideSuggestions,
    clear,
  }
}
