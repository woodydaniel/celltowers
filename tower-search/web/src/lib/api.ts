export interface Tower {
  id: number
  tower_id: string
  site_id: string
  latitude: number
  longitude: number
  provider: string
  generation: string
  site_type: string
  active: boolean
  band_labels: string[]
  tower_name: string
  tower_parent: string
  first_seen: string
  last_seen: string
  rural: boolean
  source: string
  address: string
  city: string
  state: string
  zipcode: string
  geocode_status: string
  geocode_accuracy: string
  low_precision: boolean
}

export interface ParsedFilters {
  state?: string
  city?: string
  generation?: string
  generation_prefix?: boolean
  site_type?: string
  provider?: string
  active?: boolean
  rural?: boolean
  lat?: number
  lng?: number
  radius_miles?: number
  tower_id?: string
  zipcode?: string
  fts_query?: string
}

export interface DisambiguationOption {
  city: string
  state: string
  count: number
}

export interface AmbiguousTerm {
  term: string
  field: string
  options: DisambiguationOption[]
}

export interface SearchResponse {
  parsed: ParsedFilters
  ambiguous: AmbiguousTerm[]
  results: Tower[]
  total: number
  page: number
  pages: number
  query: string
}

export interface SearchRequest {
  query: string
  page?: number
  per_page?: number
  sort_by?: string
  sort_order?: string
  resolved?: Record<string, string>
}

export interface FilterValues {
  states: string[]
  generations: string[]
  site_types: string[]
}

export interface Stats {
  total: number
  providers: Record<string, number>
  generations: Record<string, number>
  top_states: Record<string, number>
  site_types: Record<string, number>
}

// In dev: relative (proxied by Vite to localhost:8000)
// In prod: set VITE_API_BASE_URL to your Railway backend URL e.g. https://celltowers-api.up.railway.app
const BASE: string = import.meta.env.VITE_API_BASE_URL ?? ''

const TOKEN_KEY = 'ct_token'

export const auth = {
  getToken: (): string | null => sessionStorage.getItem(TOKEN_KEY),
  setToken: (t: string) => sessionStorage.setItem(TOKEN_KEY, t),
  clearToken: () => sessionStorage.removeItem(TOKEN_KEY),
  isLoggedIn: (): boolean => !!sessionStorage.getItem(TOKEN_KEY),
}

function authHeaders(): HeadersInit {
  const token = auth.getToken()
  return token ? { Authorization: `Bearer ${token}` } : {}
}

function handle401(res: Response) {
  if (res.status === 401) {
    auth.clearToken()
    window.location.reload()
  }
}

async function post<T>(path: string, body: unknown, requiresAuth = true): Promise<T> {
  const headers: HeadersInit = { 'Content-Type': 'application/json', ...(requiresAuth ? authHeaders() : {}) }
  const res = await fetch(`${BASE}${path}`, { method: 'POST', headers, body: JSON.stringify(body) })
  if (!res.ok) {
    handle401(res)
    const text = await res.text()
    throw new Error(`API error ${res.status}: ${text}`)
  }
  return res.json()
}

async function get<T>(path: string, params?: Record<string, string | number | boolean | undefined>): Promise<T> {
  const url = new URL(`${BASE}${path}`, window.location.origin)
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && v !== '') {
        url.searchParams.set(k, String(v))
      }
    }
  }
  const res = await fetch(url.toString(), { headers: authHeaders() })
  if (!res.ok) {
    handle401(res)
    const text = await res.text()
    throw new Error(`API error ${res.status}: ${text}`)
  }
  return res.json()
}

export const api = {
  async login(email: string, password: string): Promise<void> {
    const res = await post<{ access_token: string }>('/api/login', { email, password }, false)
    auth.setToken(res.access_token)
  },

  search(req: SearchRequest): Promise<SearchResponse> {
    return post('/api/search', req)
  },

  suggest(q: string): Promise<{ suggestions: string[] }> {
    return get('/api/suggest', { q })
  },

  getFilters(): Promise<FilterValues> {
    return get('/api/filters')
  },

  getStats(): Promise<Stats> {
    return get('/api/stats')
  },

  exportUrl(filters: ParsedFilters): string {
    const url = new URL('/api/towers/export', window.location.origin)
    const p = url.searchParams
    if (filters.state) p.set('state', filters.state)
    if (filters.city) p.set('city', filters.city)
    if (filters.generation) p.set('generation', filters.generation)
    if (filters.site_type) p.set('site_type', filters.site_type)
    if (filters.active !== undefined) p.set('active', String(filters.active))
    if (filters.rural !== undefined) p.set('rural', String(filters.rural))
    if (filters.lat !== undefined) p.set('lat', String(filters.lat))
    if (filters.lng !== undefined) p.set('lng', String(filters.lng))
    if (filters.radius_miles) p.set('radius_miles', String(filters.radius_miles))
    if (filters.tower_id) p.set('tower_id', filters.tower_id)
    if (filters.zipcode) p.set('zipcode', filters.zipcode)
    return url.toString()
  },
}
