import { useEffect, useState } from 'react'
import { Antenna, Radio, Loader2, LogOut } from 'lucide-react'
import { SmartSearchBar } from '@/components/SmartSearchBar'
import { DisambiguationCard } from '@/components/DisambiguationCard'
import { StatsBar } from '@/components/StatsBar'
import { ResultsTable } from '@/components/ResultsTable'
import { LoginPage } from '@/components/LoginPage'
import { useTowerSearch } from '@/hooks/useTowerSearch'
import { api, auth } from '@/lib/api'

export default function App() {
  const [isLoggedIn, setIsLoggedIn] = useState(auth.isLoggedIn())

  if (!isLoggedIn) {
    return <LoginPage onLogin={() => setIsLoggedIn(true)} />
  }

  return <MainApp onLogout={() => { auth.clearToken(); setIsLoggedIn(false) }} />
}

function MainApp({ onLogout }: { onLogout: () => void }) {
  const {
    state,
    search,
    resolveDisambiguation,
    changePage,
    changeSort,
    removeFilter,
    updateSuggestions,
    hideSuggestions,
    clear,
  } = useTowerSearch()

  const [inputValue, setInputValue] = useState('')
  const [totalTowers, setTotalTowers] = useState<number | null>(null)

  useEffect(() => {
    api.getStats().then(s => setTotalTowers(s.total)).catch(() => null)
  }, [])

  const isLoading = state.phase === 'loading'

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="sticky top-0 z-30 border-b border-border/40 bg-background/80 backdrop-blur-md">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded-lg bg-primary/20">
              <Radio className="h-5 w-5 text-primary" />
            </div>
            <div>
              <h1 className="font-semibold text-sm leading-none">Cell Tower Search</h1>
              <p className="text-xs text-muted-foreground leading-none mt-0.5">
                {totalTowers ? `${totalTowers.toLocaleString()} towers` : 'Loading…'}
              </p>
            </div>
          </div>
          <button
            onClick={onLogout}
            className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Sign out"
          >
            <LogOut className="h-3.5 w-3.5" />
            Sign out
          </button>
        </div>
      </header>

      {/* Main */}
      <main className="flex-1 max-w-7xl mx-auto w-full px-4 py-10">

        {/* Hero search area */}
        <section className="flex flex-col items-center gap-8 mb-10">
          <div className="text-center">
            <h2 className="text-3xl font-bold tracking-tight mb-2">
              Find any tower, anywhere
            </h2>
            <p className="text-muted-foreground text-sm max-w-md">
              Search 500k+ towers across AT&amp;T, T-Mobile &amp; Verizon by location, generation, site type, or tower ID.
              Natural language works.
            </p>
          </div>

          <SmartSearchBar
            value={inputValue}
            onChange={setInputValue}
            onSearch={(q) => { setInputValue(q); search(q) }}
            onSuggestionsChange={updateSuggestions}
            suggestions={state.suggestions}
            showSuggestions={state.showSuggestions}
            onHideSuggestions={hideSuggestions}
            isLoading={isLoading}
            onClear={() => { clear(); setInputValue('') }}
          />
        </section>

        {/* Disambiguation */}
        {state.phase === 'disambiguation' && (
          <div className="mb-6">
            <DisambiguationCard
              terms={state.ambiguous}
              onSelect={(term, option) => resolveDisambiguation(term, option)}
            />
          </div>
        )}

        {/* Error */}
        {state.phase === 'error' && (
          <div className="rounded-xl border border-destructive/40 bg-red-50 p-4 text-sm text-red-700 animate-fade-in">
            {state.error}
          </div>
        )}

        {/* Results */}
        {(state.phase === 'results' || (state.phase === 'loading' && state.results.length > 0)) && (
          <div className="flex flex-col gap-4 animate-fade-in">
            <StatsBar
              total={state.total}
              page={state.page}
              perPage={50}
              isLoading={isLoading}
              filters={state.parsedFilters}
              onRemoveFilter={removeFilter}
            />

            <ResultsTable
              results={state.results}
              isLoading={isLoading}
              sortBy={state.sortBy}
              sortOrder={state.sortOrder}
              onSort={(col) => {
                const newOrder = state.sortBy === col && state.sortOrder === 'asc' ? 'desc' : 'asc'
                changeSort(col, newOrder)
              }}
              page={state.page}
              pages={state.pages}
              onPage={changePage}
            />
          </div>
        )}

        {/* Loading initial (no previous results) */}
        {state.phase === 'loading' && state.results.length === 0 && (
          <div className="flex items-center justify-center py-20 animate-fade-in">
            <div className="flex flex-col items-center gap-3 text-muted-foreground">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <span className="text-sm">Searching towers…</span>
            </div>
          </div>
        )}

        {/* Empty state */}
        {state.phase === 'results' && state.results.length === 0 && (
          <div className="flex flex-col items-center justify-center py-20 text-muted-foreground animate-fade-in">
            <Antenna className="h-12 w-12 mb-4 opacity-20" />
            <p className="text-sm">No towers found. Try a different search.</p>
          </div>
        )}

      </main>

      {/* Footer */}
      <footer className="border-t border-border/30 py-4 px-4">
        <p className="text-center text-xs text-muted-foreground">
          Cell Tower Search · {new Date().getFullYear()}
        </p>
      </footer>
    </div>
  )
}
