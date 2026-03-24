import { MapPin, ChevronRight } from 'lucide-react'
import { type AmbiguousTerm, type DisambiguationOption } from '@/lib/api'
import { cn } from '@/lib/utils'

interface DisambiguationCardProps {
  terms: AmbiguousTerm[]
  onSelect: (term: AmbiguousTerm, option: DisambiguationOption) => void
}

export function DisambiguationCard({ terms, onSelect }: DisambiguationCardProps) {
  if (!terms.length) return null

  return (
    <div className="w-full max-w-3xl mx-auto animate-slide-down">
      {terms.map((term) => (
        <div key={term.term} className="rounded-xl border border-border/60 bg-card p-5 shadow-lg shadow-black/10">
          <div className="flex items-center gap-2 mb-4">
            <MapPin className="h-4 w-4 text-primary" />
            <p className="text-sm text-foreground">
              Multiple locations found for{' '}
              <span className="font-semibold text-primary">"{term.term}"</span>
              {' '}— which one did you mean?
            </p>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
            {term.options.map((opt) => (
              <button
                key={`${opt.city}-${opt.state}`}
                onClick={() => onSelect(term, opt)}
                className={cn(
                  'group flex flex-col items-start gap-1 rounded-lg p-3',
                  'border border-border/60 bg-background/60',
                  'hover:border-primary/60 hover:bg-accent',
                  'transition-all duration-150 text-left'
                )}
              >
                <div className="flex items-center justify-between w-full">
                  <span className="font-semibold text-sm text-foreground">
                    {opt.city}, {opt.state}
                  </span>
                  <ChevronRight className="h-3.5 w-3.5 text-muted-foreground group-hover:text-primary transition-colors" />
                </div>
                <span className="text-xs text-muted-foreground">
                  {opt.count.toLocaleString()} tower{opt.count !== 1 ? 's' : ''}
                </span>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
