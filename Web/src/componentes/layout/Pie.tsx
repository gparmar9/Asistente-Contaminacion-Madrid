import { useSalud } from '../../api/consultas'

export function Pie() {
  const salud = useSalud()
  const estadoApi = salud.isPending
    ? 'comprobando…'
    : salud.isError
      ? 'sin conexión'
      : `${salud.data.status} · ${salud.data.service} (${salud.data.environment})`

  return (
    <footer className="pie">
      <div className="contenedor fila-entre">
        <span>
          Trabajo Fin de Máster · Datos abiertos del Ayuntamiento de Madrid · Demo académica, no es consejo médico ni
          un aviso oficial.
        </span>
        <span className="tabular" aria-live="polite">
          API: {estadoApi}
        </span>
      </div>
    </footer>
  )
}
