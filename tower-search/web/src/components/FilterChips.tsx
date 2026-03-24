import { X } from 'lucide-react'
import { type ParsedFilters } from '@/lib/api'
import { cn } from '@/lib/utils'

interface FilterChipsProps {
  filters: ParsedFilters
  onRemove: (field: keyof ParsedFilters) => void
}

const STATE_NAMES: Record<string, string> = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas',
  CA: 'California', CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware',
  FL: 'Florida', GA: 'Georgia', HI: 'Hawaii', ID: 'Idaho',
  IL: 'Illinois', IN: 'Indiana', IA: 'Iowa', KS: 'Kansas',
  KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine', MD: 'Maryland',
  MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota', MS: 'Mississippi',
  MO: 'Missouri', MT: 'Montana', NE: 'Nebraska', NV: 'Nevada',
  NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico', NY: 'New York',
  NC: 'North Carolina', ND: 'North Dakota', OH: 'Ohio', OK: 'Oklahoma',
  OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island', SC: 'South Carolina',
  SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas', UT: 'Utah',
  VT: 'Vermont', VA: 'Virginia', WA: 'Washington', WV: 'West Virginia',
  WI: 'Wisconsin', WY: 'Wyoming', DC: 'District of Columbia',
}

type ChipDef = { field: keyof ParsedFilters; label: string; color?: string }

function buildChips(f: ParsedFilters): ChipDef[] {
  const chips: ChipDef[] = []
  if (f.generation) {
    const color = f.generation.startsWith('5G') ? 'chip-5g' : 'chip-4g'
    chips.push({ field: 'generation', label: f.generation, color })
  }
  if (f.provider) chips.push({ field: 'provider', label: f.provider, color: 'chip-provider' })
  if (f.site_type) chips.push({ field: 'site_type', label: f.site_type })
  if (f.city) chips.push({ field: 'city', label: f.city })
  if (f.state) chips.push({ field: 'state', label: STATE_NAMES[f.state] ?? f.state })
  if (f.zipcode) chips.push({ field: 'zipcode', label: `ZIP ${f.zipcode}` })
  if (f.tower_id) chips.push({ field: 'tower_id', label: `ID: ${f.tower_id}` })
  if (f.active === true)  chips.push({ field: 'active', label: 'Active' })
  if (f.active === false) chips.push({ field: 'active', label: 'Inactive' })
  if (f.rural === true)   chips.push({ field: 'rural', label: 'Rural' })
  if (f.rural === false)  chips.push({ field: 'rural', label: 'Urban' })
  if (f.lat != null && f.lng != null) {
    chips.push({
      field: 'lat',
      label: `${f.lat.toFixed(4)}, ${f.lng.toFixed(4)} (${f.radius_miles ?? 5}mi)`,
    })
  }
  if (f.fts_query) chips.push({ field: 'fts_query', label: `"${f.fts_query}"` })
  return chips
}

export function FilterChips({ filters, onRemove }: FilterChipsProps) {
  const chips = buildChips(filters)
  if (!chips.length) return null

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <span className="text-xs text-muted-foreground">Filters:</span>
      {chips.map(({ field, label, color }) => (
        <span
          key={field}
          className={cn(
            'inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium',
            'border border-border/60 bg-secondary text-secondary-foreground',
            color === 'chip-5g' && 'border-violet-300 bg-violet-50 text-violet-700',
            color === 'chip-4g' && 'border-blue-300 bg-blue-50 text-blue-700',
            color === 'chip-provider' && 'border-teal-300 bg-teal-50 text-teal-700',
          )}
        >
          {label}
          <button
            onClick={() => onRemove(field)}
            className="hover:text-foreground ml-0.5 opacity-60 hover:opacity-100 transition-opacity"
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
    </div>
  )
}
