import { Link, NavLink } from 'react-router-dom'

import { useTema } from '../../estado/hooks'
import { BannerAviso } from './BannerAviso'

const SECCIONES = [
  { ruta: '/', etiqueta: 'Home' },
  { ruta: '/estadisticas', etiqueta: 'Estadísticas' },
  { ruta: '/estaciones', etiqueta: 'Estaciones' },
] as const

/** Cabecera fija: marca, navegación, tema y, debajo, el banner de aviso médico. */
export function Cabecera() {
  const { tema, alternar } = useTema()
  const esOscuro = tema === 'oscuro'

  return (
    <header className="cabecera">
      <div className="contenedor cabecera-barra">
        <Link to="/" className="marca" aria-label="Madrid Respira, inicio">
          <img
            src={esOscuro ? '/img/marca-oscuro.png' : '/img/marca.png'}
            alt="Madrid Respira — Asistente de Calidad del Aire"
            className="marca-img"
          />
        </Link>

        <nav className="nav" aria-label="Secciones">
          {SECCIONES.map((s) => (
            <NavLink key={s.ruta} to={s.ruta} end={s.ruta === '/'}>
              {s.etiqueta}
            </NavLink>
          ))}
        </nav>

        <button
          type="button"
          className="boton boton-icono tema-boton"
          onClick={alternar}
          aria-pressed={esOscuro}
          aria-label={esOscuro ? 'Cambiar a modo claro' : 'Cambiar a modo oscuro'}
          title={esOscuro ? 'Modo claro' : 'Modo oscuro'}
        >
          {esOscuro ? (
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
              <circle cx="12" cy="12" r="4" />
              <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
            </svg>
          ) : (
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
              <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />
            </svg>
          )}
        </button>
      </div>
      <BannerAviso />
    </header>
  )
}
