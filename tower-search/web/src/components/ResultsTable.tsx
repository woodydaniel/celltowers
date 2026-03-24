import { ArrowUpDown, ArrowUp, ArrowDown, ExternalLink } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { type Tower } from '@/lib/api'
import { cn } from '@/lib/utils'

interface Column {
  key: string
  label: string
  sortable?: boolean
  className?: string
}

const COLUMNS: Column[] = [
  { key: 'site_id',    label: 'Site ID',    sortable: false, className: 'font-mono w-24' },
  { key: 'provider',  label: 'Carrier',    sortable: true,  className: 'w-28' },
  { key: 'address',   label: 'Address',    sortable: false, className: 'min-w-40' },
  { key: 'city',      label: 'City',       sortable: true,  className: 'w-32' },
  { key: 'state',     label: 'ST',         sortable: true,  className: 'w-10 text-center' },
  { key: 'generation',label: 'Gen',        sortable: true,  className: 'w-28' },
  { key: 'site_type', label: 'Type',       sortable: true,  className: 'w-32' },
  { key: 'active',    label: 'Status',     sortable: true,  className: 'w-20 text-center' },
  { key: 'first_seen',label: 'First Seen', sortable: true,  className: 'w-24 text-center' },
  { key: 'last_seen', label: 'Last Seen',  sortable: true,  className: 'w-24 text-center' },
]

function providerColor(provider: string) {
  const p = provider.toLowerCase()
  if (p.includes('t-mobile') || p.includes('tmobile')) return 'bg-pink-100 text-pink-700'
  if (p.includes('verizon')) return 'bg-red-100 text-red-700'
  if (p.includes('at&t') || p.includes('att')) return 'bg-blue-100 text-blue-700'
  return 'bg-gray-100 text-gray-600'
}

function generationVariant(gen: string) {
  if (gen.startsWith('5G')) return '5g' as const
  if (gen.startsWith('4G Advanced')) return '4g-adv' as const
  if (gen.startsWith('4G')) return '4g' as const
  return 'secondary' as const
}

interface ResultsTableProps {
  results: Tower[]
  isLoading: boolean
  sortBy: string
  sortOrder: 'asc' | 'desc'
  onSort: (col: string) => void
  page: number
  pages: number
  onPage: (p: number) => void
}

export function ResultsTable({
  results,
  isLoading,
  sortBy,
  sortOrder,
  onSort,
  page,
  pages,
  onPage,
}: ResultsTableProps) {
  const SortIcon = ({ col }: { col: string }) => {
    if (col !== sortBy) return <ArrowUpDown className="h-3.5 w-3.5 opacity-40" />
    return sortOrder === 'asc'
      ? <ArrowUp className="h-3.5 w-3.5 text-primary" />
      : <ArrowDown className="h-3.5 w-3.5 text-primary" />
  }

  const mapUrl = (tower: Tower) => {
    const tid = tower.tower_id
    const parts = tid.split('_')
    if (parts.length >= 3) {
      return `https://www.cellmapper.net/map?MCC=${parts[0]}&MNC=${parts[1]}&type=LTE&latitude=${tower.latitude}&longitude=${tower.longitude}&zoom=14`
    }
    return `https://www.cellmapper.net/map?latitude=${tower.latitude}&longitude=${tower.longitude}&zoom=14`
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Table */}
      <div className="rounded-xl border border-border/60 overflow-hidden shadow-sm">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border/60 bg-muted/40">
                {COLUMNS.map((col) => (
                  <th
                    key={col.key}
                    className={cn(
                      'px-3 py-3 text-left text-xs font-semibold uppercase tracking-wide text-muted-foreground',
                      col.className,
                      col.sortable && 'cursor-pointer select-none hover:text-foreground transition-colors'
                    )}
                    onClick={() => col.sortable && onSort(col.key)}
                  >
                    <div className="flex items-center gap-1">
                      {col.label}
                      {col.sortable && <SortIcon col={col.key} />}
                    </div>
                  </th>
                ))}
                <th className="px-3 py-3 w-10" />
              </tr>
            </thead>
            <tbody className="divide-y divide-border/40">
              {isLoading
                ? Array.from({ length: 10 }).map((_, i) => (
                    <tr key={i} className="bg-background/30">
                      {COLUMNS.map((col) => (
                        <td key={col.key} className={cn('px-3 py-3', col.className)}>
                          <Skeleton className="h-4 w-full" />
                        </td>
                      ))}
                      <td className="px-3 py-3"><Skeleton className="h-4 w-6" /></td>
                    </tr>
                  ))
                : results.map((tower) => (
                    <tr
                      key={tower.id}
                      className="bg-background/20 hover:bg-accent/30 transition-colors"
                    >
                      <td className="px-3 py-3 font-mono text-xs text-muted-foreground">
                        {tower.site_id || tower.tower_id}
                      </td>
                      <td className="px-3 py-3">
                        {tower.provider ? (
                          <span className={cn(
                            'inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium',
                            providerColor(tower.provider)
                          )}>
                            {tower.provider}
                          </span>
                        ) : <span className="text-muted-foreground/50">—</span>}
                      </td>
                      <td className="px-3 py-3 text-foreground/90">
                        {tower.address || <span className="text-muted-foreground/50">—</span>}
                      </td>
                      <td className="px-3 py-3">{tower.city}</td>
                      <td className="px-3 py-3 text-center font-medium text-xs">{tower.state}</td>
                      <td className="px-3 py-3">
                        <Badge variant={generationVariant(tower.generation)}>
                          {tower.generation}
                        </Badge>
                      </td>
                      <td className="px-3 py-3 text-foreground/80">{tower.site_type}</td>
                      <td className="px-3 py-3 text-center">
                        <span className={cn(
                          'inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium',
                          tower.active
                            ? 'bg-emerald-100 text-emerald-700'
                            : 'bg-slate-100 text-slate-500'
                        )}>
                          {tower.active ? 'Active' : 'Inactive'}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-center text-xs text-muted-foreground">
                        {tower.first_seen || '—'}
                      </td>
                      <td className="px-3 py-3 text-center text-xs text-muted-foreground">
                        {tower.last_seen || '—'}
                      </td>
                      <td className="px-3 py-3">
                        <a
                          href={mapUrl(tower)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-muted-foreground hover:text-primary transition-colors"
                          title="View on map"
                        >
                          <ExternalLink className="h-3.5 w-3.5" />
                        </a>
                      </td>
                    </tr>
                  ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {pages > 1 && (
        <div className="flex items-center justify-center gap-1">
          <Button
            variant="outline"
            size="sm"
            onClick={() => onPage(page - 1)}
            disabled={page <= 1 || isLoading}
          >
            ←
          </Button>

          {buildPageNumbers(page, pages).map((p, i) =>
            p === null ? (
              <span key={`ellipsis-${i}`} className="px-2 text-muted-foreground">…</span>
            ) : (
              <Button
                key={p}
                variant={p === page ? 'default' : 'outline'}
                size="sm"
                className="w-9"
                onClick={() => onPage(p)}
                disabled={isLoading}
              >
                {p}
              </Button>
            )
          )}

          <Button
            variant="outline"
            size="sm"
            onClick={() => onPage(page + 1)}
            disabled={page >= pages || isLoading}
          >
            →
          </Button>
        </div>
      )}
    </div>
  )
}

function buildPageNumbers(current: number, total: number): (number | null)[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1)
  const pages: (number | null)[] = [1]
  if (current > 3) pages.push(null)
  for (let p = Math.max(2, current - 1); p <= Math.min(total - 1, current + 1); p++) {
    pages.push(p)
  }
  if (current < total - 2) pages.push(null)
  pages.push(total)
  return pages
}
