import { Download, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { FilterChips } from '@/components/FilterChips'
import { type ParsedFilters } from '@/lib/api'
import { api } from '@/lib/api'

interface StatsBarProps {
  total: number
  page: number
  perPage: number
  isLoading: boolean
  filters: ParsedFilters | null
  onRemoveFilter: (field: keyof ParsedFilters) => void
}

export function StatsBar({
  total,
  page,
  perPage,
  isLoading,
  filters,
  onRemoveFilter,
}: StatsBarProps) {
  const from = (page - 1) * perPage + 1
  const to = Math.min(page * perPage, total)

  const handleExport = () => {
    if (!filters) return
    const url = api.exportUrl(filters)
    const a = document.createElement('a')
    a.href = url
    a.download = 'att_towers_export.csv'
    a.click()
  }

  return (
    <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex flex-col gap-1.5">
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          {isLoading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <span>
              Showing{' '}
              <span className="text-foreground font-medium">
                {from.toLocaleString()}–{to.toLocaleString()}
              </span>{' '}
              of{' '}
              <span className="text-foreground font-medium">
                {total.toLocaleString()}
              </span>{' '}
              tower{total !== 1 ? 's' : ''}
            </span>
          )}
        </div>

        {filters && (
          <FilterChips filters={filters} onRemove={onRemoveFilter} />
        )}
      </div>

      <Button
        variant="outline"
        size="sm"
        className="gap-1.5 self-start sm:self-auto"
        onClick={handleExport}
        disabled={!filters || total === 0}
      >
        <Download className="h-3.5 w-3.5" />
        Export CSV
      </Button>
    </div>
  )
}
