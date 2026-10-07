import { useAviso } from '../../estado/hooks'

/**
 * Aviso médico persistente: forma parte de la cabecera fija, así que se ve sin
 * hacer scroll en todas las pantallas (requisito evaluado del TFM).
 */
export function BannerAviso() {
  const { texto } = useAviso()
  return (
    <div className="banner-aviso" role="note" aria-label="Aviso médico">
      <div className="contenedor" style={{ display: 'flex', alignItems: 'center', gap: 8, padding: 0 }}>
        <svg className="banner-aviso-icono" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
          <circle cx="12" cy="12" r="10" />
          <path d="M12 8h.01M11 12h1v4h1" />
        </svg>
        <span>
          <strong>Aviso médico:</strong> {texto}
        </span>
      </div>
    </div>
  )
}
