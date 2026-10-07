import { useCronometro } from '../../hooks/useCronometro'

interface Props {
  activo: boolean
  texto?: string
}

/** Contador de segundos mientras se espera al asistente (puede llegar a ~120 s). */
export function Espera({ activo, texto = 'Esperando la respuesta del asistente' }: Props) {
  const segundos = useCronometro(activo)
  if (!activo) return null
  return (
    <p className="cargando" role="status" aria-live="polite">
      {texto}… <span className="contador">{segundos} s</span>
      <span className="texto-apagado">· puede tardar hasta 2 minutos</span>
    </p>
  )
}
