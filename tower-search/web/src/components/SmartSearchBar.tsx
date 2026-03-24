import React, { useRef, useEffect } from 'react'
import { Search, X, Loader2 } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

const EXAMPLES = [
  '5G towers in Florida',
  'T-Mobile in New York',
  'Verizon towers in Texas',
  '39.90, -74.21',
  'Tower ID 811184',
  'Active small cells in Denver',
]

interface SmartSearchBarProps {
  value: string
  onChange: (val: string) => void
  onSearch: (val: string) => void
  onSuggestionsChange: (val: string) => void
  suggestions: string[]
  showSuggestions: boolean
  onHideSuggestions: () => void
  isLoading: boolean
  onClear: () => void
}

export function SmartSearchBar({
  value,
  onChange,
  onSearch,
  onSuggestionsChange,
  suggestions,
  showSuggestions,
  onHideSuggestions,
  isLoading,
  onClear,
}: SmartSearchBarProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  const handleInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = e.target.value
    onChange(v)
    onSuggestionsChange(v)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && value.trim()) {
      onHideSuggestions()
      onSearch(value.trim())
    }
    if (e.key === 'Escape') {
      onHideSuggestions()
    }
  }

  const handleSuggestionClick = (s: string) => {
    onChange(s)
    onHideSuggestions()
    onSearch(s)
    inputRef.current?.focus()
  }

  const handleExampleClick = (ex: string) => {
    onChange(ex)
    onSearch(ex)
  }

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        onHideSuggestions()
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [onHideSuggestions])

  return (
    <div className="w-full max-w-3xl mx-auto">
      {/* Search input */}
      <div ref={containerRef} className="relative">
        <div className="relative flex items-center">
          <div className="pointer-events-none absolute left-4 text-muted-foreground">
            {isLoading
              ? <Loader2 className="h-5 w-5 animate-spin text-primary" />
              : <Search className="h-5 w-5" />
            }
          </div>

          <Input
            ref={inputRef}
            value={value}
            onChange={handleInput}
            onKeyDown={handleKeyDown}
            onFocus={() => value.length >= 2 && onSuggestionsChange(value)}
            placeholder='Search towers…'
            className="h-14 pl-12 pr-28 text-base rounded-xl border-border bg-white focus-visible:ring-primary/60 shadow-md shadow-gray-200"
            autoFocus
          />

          <div className="absolute right-2 flex gap-1">
            {value && (
              <Button
                variant="ghost"
                size="icon"
                className="h-9 w-9 text-muted-foreground hover:text-foreground"
                onClick={() => { onClear(); onChange('') }}
              >
                <X className="h-4 w-4" />
              </Button>
            )}
            <Button
              size="sm"
              className="px-4 rounded-lg"
              onClick={() => value.trim() && onSearch(value.trim())}
              disabled={!value.trim() || isLoading}
            >
              Search
            </Button>
          </div>
        </div>

        {/* Autocomplete dropdown */}
        {showSuggestions && suggestions.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 z-50 rounded-xl border border-border bg-white shadow-xl shadow-gray-200/80 animate-slide-down overflow-hidden">
            {suggestions.map((s) => (
              <button
                key={s}
                className="w-full text-left px-4 py-2.5 text-sm hover:bg-accent transition-colors flex items-center gap-2"
                onMouseDown={(e) => { e.preventDefault(); handleSuggestionClick(s) }}
              >
                <Search className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                {s}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Example queries */}
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <span className="text-xs text-muted-foreground">Try:</span>
        {EXAMPLES.map((ex) => (
          <button
            key={ex}
            onClick={() => handleExampleClick(ex)}
            className={cn(
              'text-xs px-2.5 py-1 rounded-full border border-border/60',
              'text-muted-foreground hover:text-foreground hover:border-border',
              'hover:bg-accent transition-all duration-150'
            )}
          >
            {ex}
          </button>
        ))}
      </div>
    </div>
  )
}
